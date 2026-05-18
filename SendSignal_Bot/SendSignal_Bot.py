import asyncio
import threading
import pandas as pd
import sys
from flask import Flask, request, jsonify
from telethon import TelegramClient
from datetime import datetime

app = Flask(__name__)

loop   = None
client = None
cfg    = None


# --- CẤU HÌNH ---
def load_config(file_path="Data.xlsx"):
    try:
        df = pd.read_excel(file_path)
        dest_ids = [int(x.strip()) for x in str(df["Danh_Sách_ID_Nhận"].iloc[0]).split(",")]
        config = {
            "api_id":   int(df["Mã_API"].iloc[0]),
            "api_hash": str(df["Chuỗi_API"].iloc[0]).strip(),
            "dest_ids": dest_ids,
            "port":     int(df["Port"].iloc[0]) if "Port" in df.columns else 5000,
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


# --- ĐỊNH DẠNG TIN NHẮN ---
def format_message(data: dict) -> str:
    signal   = data.get("signal",   "")
    ticker   = data.get("ticker",   "N/A")
    close    = data.get("close",    "N/A")
    interval = data.get("interval", "N/A")
    exchange = data.get("exchange", "N/A")

    sig_upper = signal.upper()
    if "LONG" in sig_upper:
        icon   = "🟢"
        action = "LONG ▲"
    elif "SHORT" in sig_upper:
        icon   = "🔴"
        action = "SHORT ▼"
    else:
        icon   = "⚪"
        action = signal

    now = datetime.now().strftime("%H:%M  %d/%m/%Y")
    return (
        f"{icon} <b>UT {action}</b> | <b>{ticker}</b>\n"
        f"📊 Sàn: {exchange}  |  ⏱ Khung: {interval}\n"
        f"💰 Giá đóng cửa: {close}\n"
        f"🕐 {now}"
    )


# --- GỬI TỚI TẤT CẢ NHÓM ---
async def send_all(message: str):
    for chat_id in cfg["dest_ids"]:
        try:
            await client.send_message(int(chat_id), message, parse_mode="html")
            print(f"  ✅ Gửi thành công -> {chat_id}")
        except Exception as e:
            print(f"  ❌ Lỗi tại {chat_id}: {type(e).__name__} — {e}")


# --- WEBHOOK ENDPOINT ---
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    signal = data.get("signal", "unknown")
    print(f"\n📡 Nhận tín hiệu: {signal}  [{datetime.now().strftime('%H:%M:%S')}]")

    msg = format_message(data)
    asyncio.run_coroutine_threadsafe(send_all(msg), loop)

    return jsonify({"status": "ok", "signal": signal}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"}), 200


# --- CHƯƠNG TRÌNH CHÍNH ---
async def main():
    global loop, client, cfg

    cfg    = load_config()
    client = TelegramClient(
        "signal_session",
        cfg["api_id"],
        cfg["api_hash"],
        connection_retries=None,
        auto_reconnect=True,
    )
    loop = asyncio.get_running_loop()

    await client.start()
    me = await client.get_me()

    print("\n" + "=" * 50)
    print(f"✅ Đăng nhập: {me.first_name}")
    print(f"🎯 Đích: {len(cfg['dest_ids'])} nhóm/kênh")
    print(f"🌐 Webhook: http://0.0.0.0:{cfg['port']}/webhook")
    print(f"❤️  Health:  http://0.0.0.0:{cfg['port']}/health")
    print("=" * 50 + "\n")

    flask_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=cfg["port"], use_reloader=False),
        daemon=True,
    )
    flask_thread.start()

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng bot.")
