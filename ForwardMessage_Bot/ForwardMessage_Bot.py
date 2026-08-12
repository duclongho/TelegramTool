import asyncio
import pandas as pd
import sys
from telethon import TelegramClient, events, errors
from deep_translator import GoogleTranslator

names_cache = {}


# --- HÀM ĐỌC CẤU HÌNH ---
def load_config(file_path="Data.xlsx"):
    try:
        df = pd.read_excel(file_path)

        api_id = int(df["Mã_API"].iloc[0])
        api_hash = str(df["Chuỗi_API"].iloc[0]).strip()

        routes = []  # Mỗi phần tử: {source_id, src_lang, dests: [(dest_id, target_lang), ...]}
        routing = {}  # source_id -> [{dest_id, src_lang, target_lang}, ...] (gộp nếu 1 nguồn xuất hiện nhiều dòng)

        for idx, row in df.iterrows():
            dong = idx + 2  # Số dòng thực tế trong Excel (tính cả header)

            if pd.isna(row["ID_Nguồn"]):
                continue  # Bỏ qua dòng trống

            source_id = int(row["ID_Nguồn"])
            src_lang = str(row["Ngôn_Ngữ_Gốc"]).strip().lower()

            dest_ids = [
                int(x.strip()) for x in str(row["Danh_Sách_ID_Nhận"]).split(",")
            ]
            dest_langs = [
                x.strip().lower() for x in str(row["Ngôn_Ngữ_Dịch"]).split(",")
            ]

            if len(dest_ids) != len(dest_langs):
                print(
                    f"❌ LỖI: Dòng {dong}: Số lượng ID nhận ({len(dest_ids)}) không khớp "
                    f"với số lượng ngôn ngữ dịch ({len(dest_langs)}) trong Excel!"
                )
                sys.exit(1)

            dests = list(zip(dest_ids, dest_langs))
            routes.append(
                {"source_id": source_id, "src_lang": src_lang, "dests": dests}
            )
            routing.setdefault(source_id, []).extend(
                {"dest_id": d, "src_lang": src_lang, "target_lang": t}
                for d, t in dests
            )

        if not routes:
            print("❌ LỖI: Không có dòng cấu hình hợp lệ nào trong Excel!")
            sys.exit(1)

        config = {
            "api_id": api_id,
            "api_hash": api_hash,
            "source_ids": sorted(routing.keys()),
            "routes": routes,
            "routing": routing,
        }
        return config
    except FileNotFoundError:
        print(f"❌ LỖI: Không tìm thấy file '{file_path}'")
        sys.exit(1)
    except PermissionError:
        print(f"❌ LỖI: File '{file_path}' đang mở trong Excel. Hãy đóng nó lại!")
        sys.exit(1)
    except KeyError as e:
        print(f"❌ LỖI CẤU HÌNH: Thiếu cột {e} trong Excel!")
        sys.exit(1)
    except Exception as e:
        print(f"❌ LỖI CẤU HÌNH: {e}")
        sys.exit(1)


# --- KHỞI TẠO ---
cfg = load_config()
client = TelegramClient(
    "my_session",
    cfg["api_id"],
    cfg["api_hash"],
    connection_retries=None,  # Thử lại vô hạn khi mất mạng
    auto_reconnect=True,
)


# --- HÀM DỊCH THUẬT ---
def translate_text(text, src, tgt):
    try:
        if not text or len(text.strip()) == 0:
            return text
        if src == tgt:
            return text  # Nếu trùng ngôn ngữ thì không cần dịch
        return GoogleTranslator(source=src, target=tgt).translate(text)
    except Exception as e:
        print(f"⚠️ Lỗi dịch sang {tgt}: {e}")
        return text


# --- XỬ LÝ CHUYỂN TIẾP ---
@client.on(events.NewMessage(chats=cfg["source_ids"]))
async def handler(event):
    if not event.raw_text:
        return

    src_id = event.chat_id
    dest_routes = cfg["routing"].get(src_id, [])
    if not dest_routes:
        return

    src_name = names_cache.get(src_id, str(src_id))
    print(f"\n📩 Tin nhắn mới từ [{src_name}], đang xử lý chuyển tiếp...")

    tasks = []
    for r in dest_routes:
        dest_name = names_cache.get(r["dest_id"], str(r["dest_id"]))
        translated_text = translate_text(
            event.raw_text, r["src_lang"], r["target_lang"]
        )
        tasks.append(
            send_to_group(r["dest_id"], dest_name, translated_text, event.media)
        )

    await asyncio.gather(*tasks)


async def send_to_group(chat_id, name, message, media):
    try:
        await client.send_message(chat_id, message, file=media)
        print(f" ✅ Gửi thành công -> {name}")
    except errors.FloodWaitError as e:
        print(f" ⏳ Bị giới hạn tốc độ. Chờ {e.seconds} giây...")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        print(f" ❌ Lỗi tại nhóm {chat_id}: {type(e).__name__}")


# --- CHƯƠNG TRÌNH CHÍNH ---
async def main():
    try:
        # Kiểm tra kết nối
        await client.start()

        me = await client.get_me()
        dest_ids = {d for r in cfg["routes"] for d, _ in r["dests"]}

        print("\n" + "=" * 48)
        print(f"✅ BOT ĐANG CHẠY DƯỚI TÊN: {me.first_name}")
        print(f"🔧 Số cấu hình (dòng): {len(cfg['routes'])}")
        print(f"📤 Tổng số địa chỉ gửi (nguồn): {len(cfg['source_ids'])}")
        print(f"📥 Tổng số địa chỉ nhận (đích): {len(dest_ids)}")
        print("=" * 48)

        # --- TỰ ĐỘNG LẤY TÊN KHI KHỞI ĐỘNG (cả nguồn lẫn đích) ---#
        all_ids = set(cfg["source_ids"]) | dest_ids
        for cid in all_ids:
            try:
                entity = await client.get_entity(cid)
                names_cache[cid] = getattr(entity, "title", None) or getattr(
                    entity, "first_name", str(cid)
                )
            except Exception:
                names_cache[cid] = str(cid)

        # In chi tiết từng cấu hình định tuyến
        for r in cfg["routes"]:
            src_name = names_cache.get(r["source_id"], str(r["source_id"]))
            dest_names = ", ".join(
                f"{names_cache.get(d, str(d))}({lang})" for d, lang in r["dests"]
            )
            print(f"   📤 {src_name} ({r['src_lang']}) ➜ 📥 [{dest_names}]")

        await client.run_until_disconnected()
    except Exception as e:
        print(f"💥 Lỗi hệ thống: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng bot.")
