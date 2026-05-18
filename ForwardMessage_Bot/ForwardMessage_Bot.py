import asyncio
import pandas as pd
import sys
from telethon import TelegramClient, events, errors
from deep_translator import GoogleTranslator

group_names_cache = {}


# --- HÀM ĐỌC CẤU HÌNH ---
def load_config(file_path="Data.xlsx"):
    try:
        df = pd.read_excel(file_path)

        # Tách danh sách ID và danh sách ngôn ngữ
        dest_ids = [
            int(x.strip()) for x in str(df["Danh_Sách_ID_Nhận"].iloc[0]).split(",")
        ]
        dest_langs = [
            x.strip().lower() for x in str(df["Ngôn_Ngữ_Dịch"].iloc[0]).split(",")
        ]

        # Kiểm tra tính hợp lệ
        if len(dest_ids) != len(dest_langs):
            print(
                f"❌ LỖI: Số lượng ID nhóm ({len(dest_ids)}) không khớp với số lượng ngôn ngữ ({len(dest_langs)}) trong Excel!"
            )
            sys.exit(1)

        config = {
            "api_id": int(df["Mã_API"].iloc[0]),
            "api_hash": str(df["Chuỗi_API"].iloc[0]).strip(),
            "source_id": int(df["ID_Nguồn"].iloc[0]),
            "dest_ids": dest_ids,
            "src_lang": str(df["Ngôn_Ngữ_Gốc"].iloc[0]).strip().lower(),
            # Tạo danh sách các cặp (ID, Ngôn ngữ)
            "dest_list": list(zip(dest_ids, dest_langs)),
        }
        return config
    except FileNotFoundError:
        print(f"❌ LỖI: Không tìm thấy file '{file_path}'")
        sys.exit(1)
    except PermissionError:
        print(f"❌ LỖI: File '{file_path}' đang mở trong Excel. Hãy đóng nó lại!")
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
@client.on(events.NewMessage(chats=cfg["source_id"]))
async def handler(event):
    if not event.raw_text:
        return

    print(f"\n📩 Tin nhắn mới, đang xử lý chuyển tiếp...")

    tasks = []
    for out_id, target_lang in cfg["dest_list"]:
        name = group_names_cache.get(out_id, str(out_id))
        translated_text = translate_text(event.raw_text, cfg["src_lang"], target_lang)
        tasks.append(send_to_group(out_id, name, translated_text, event.media))

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
        source_entity = await client.get_entity(cfg["source_id"])

        print("\n" + "=" * 48)
        print(f"✅ BOT ĐANG CHẠY DƯỚI TÊN: {me.first_name}")
        print(f"📢 Nguồn gửi: {source_entity.title}")
        print(f"🎯 Đích đến: {len(cfg['dest_ids'])} nhóm")
        print("=" * 48)

        # --- TỰ ĐỘNG LẤY TÊN NHÓM KHI KHỞI ĐỘNG ---#
        for out_id, _ in cfg["dest_list"]:
            try:
                entity = await client.get_entity(out_id)
                group_names_cache[out_id] = entity.title
            except:
                group_names_cache[out_id] = f"Nhóm {out_id}"

        await client.run_until_disconnected()
    except Exception as e:
        print(f"💥 Lỗi hệ thống: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng bot.")
