import asyncio
import threading
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')
import json
import os
from flask import Flask, request, jsonify
from telethon import TelegramClient
from datetime import datetime, timedelta

app = Flask(__name__)

loop          = None
client        = None
cfg           = None
open_trades   = {}   # ticker -> trade dict (lệnh đang chạy)
closed_trades = []   # list (lệnh đã đóng trong ngày)

TRADES_FILE = "trades.json"


# --- LƯU / TẢI DỮ LIỆU ---
def save_trades():
    data = {
        "date":          datetime.now().strftime("%Y-%m-%d"),
        "open_trades":   open_trades,
        "closed_trades": closed_trades,
    }
    with open(TRADES_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)


def load_trades():
    global open_trades, closed_trades
    if not os.path.exists(TRADES_FILE):
        return

    try:
        with open(TRADES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Chỉ load nếu dữ liệu là của ngày hôm nay
        if data.get("date") != datetime.now().strftime("%Y-%m-%d"):
            print("📅 Dữ liệu cũ (khác ngày), bỏ qua.")
            os.remove(TRADES_FILE)
            return

        open_trades   = data.get("open_trades", {})
        closed_trades = data.get("closed_trades", [])
        print(f"♻️  Khôi phục: {len(open_trades)} lệnh đang mở, {len(closed_trades)} lệnh đã đóng.")
    except Exception as e:
        print(f"⚠️ Không đọc được trades.json: {e}")


# --- CẤU HÌNH ---
def load_config(file_path="Data.xlsx"):
    try:
        df = pd.read_excel(file_path)
        dest_ids = [int(x.strip()) for x in str(df["Danh_Sách_ID_Nhận"].iloc[0]).split(",")]
        config = {
            "api_id":       int(df["Mã_API"].iloc[0]),
            "api_hash":     str(df["Chuỗi_API"].iloc[0]).strip(),
            "dest_ids":     dest_ids,
            "port":         int(df["Port"].iloc[0]) if "Port" in df.columns else 80,
            "token":        str(df["Token"].iloc[0]).strip(),
            "summary_time": str(df["Giờ_Tổng_Kết"].iloc[0]).strip() if "Giờ_Tổng_Kết" in df.columns else "23:59",
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


# --- GHI NHẬN LỆNH MỞ ---
def record_trade_open(data: dict):
    ticker    = data.get("ticker", "N/A")
    sig_upper = data.get("signal", "").upper()
    direction = "LONG" if "LONG" in sig_upper else "SHORT"

    try:
        entry_price = float(data.get("close", 0))
        tp_price    = float(data.get("tp", 0)) if data.get("tp") else None
        sl_price    = float(data.get("sl", 0)) if data.get("sl") else None
    except (ValueError, TypeError):
        entry_price, tp_price, sl_price = 0.0, None, None

    try:
        tp1 = float(data.get("tp1", 0)) if data.get("tp1") else None
        tp2 = float(data.get("tp2", 0)) if data.get("tp2") else None
        tp3 = float(data.get("tp3", 0)) if data.get("tp3") else None
        sl  = float(data.get("sl",  0)) if data.get("sl")  else None
    except (ValueError, TypeError):
        tp1 = tp2 = tp3 = sl = None

    open_trades[ticker] = {
        "ticker":      ticker,
        "direction":   direction,
        "entry_price": entry_price,
        "tp1":         tp1,
        "tp2":         tp2,
        "tp3":         tp3,
        "sl":          sl,
        "entry_time":  datetime.now().isoformat(),
        "exit_price":  None,
        "exit_time":   None,
        "result":      None,
        "pnl":         None,
    }
    save_trades()


# --- GHI NHẬN LỆNH ĐÓNG ---
def record_trade_close(data: dict):
    ticker    = data.get("ticker", "N/A")
    sig_upper = data.get("signal", "").upper()
    if "TP3" in sig_upper:
        result = "TP3"
    elif "TP2" in sig_upper:
        result = "TP2"
    elif "TP1" in sig_upper:
        result = "TP1"
    else:
        result = "SL"

    trade = open_trades.pop(ticker, None)
    if not trade:
        return

    try:
        exit_price = float(data.get("hit_price", data.get("close", 0)))
    except (ValueError, TypeError):
        exit_price = 0.0

    pnl = abs(exit_price - trade["entry_price"])
    if result == "SL":
        pnl = -pnl

    trade.update({
        "exit_price": exit_price,
        "exit_time":  datetime.now().isoformat(),
        "result":     result,
        "pnl":        round(pnl, 2),
    })
    closed_trades.append(trade)
    save_trades()


# --- CHUYỂN ĐỔI KHUNG THỜI GIAN ---
def format_interval(tf: str) -> str:
    if tf.isdigit():
        minutes = int(tf)
        if minutes < 60:
            return f"{minutes}m"
        elif minutes % 60 == 0:
            return f"{minutes // 60}h"
        else:
            return f"{minutes}m"
    mapping = {"D": "1D", "W": "1W", "M": "1M"}
    return mapping.get(tf.upper(), tf)


# --- ĐỊNH DẠNG TIN NHẮN TÍN HIỆU ---
def format_message(data: dict) -> str:
    signal    = data.get("signal",    "")
    ticker    = data.get("ticker",    "N/A")
    close     = data.get("close",     "N/A")
    interval  = data.get("interval",  "N/A")
    tp1       = data.get("tp1",       None)
    tp2       = data.get("tp2",       None)
    tp3       = data.get("tp3",       None)
    sl        = data.get("sl",        None)
    hit_price = data.get("hit_price", None)

    sig_upper = signal.upper()
    if "LONG TP3" in sig_upper:
        icon, action = "💰", "BUY TP3 ✅✅✅"
    elif "LONG TP2" in sig_upper:
        icon, action = "🎯", "BUY TP2 ✅✅"
    elif "LONG TP1" in sig_upper:
        icon, action = "✅", "BUY TP1 ✅"
    elif "LONG SL" in sig_upper:
        icon, action = "🛑", "BUY SL ❌"
    elif "SHORT TP3" in sig_upper:
        icon, action = "💰", "SELL TP3 ✅✅✅"
    elif "SHORT TP2" in sig_upper:
        icon, action = "🎯", "SELL TP2 ✅✅"
    elif "SHORT TP1" in sig_upper:
        icon, action = "✅", "SELL TP1 ✅"
    elif "SHORT SL" in sig_upper:
        icon, action = "🛑", "SELL SL ❌"
    elif "LONG" in sig_upper:
        icon, action = "🟢", "BUY  ▲"
    elif "SHORT" in sig_upper:
        icon, action = "🔴", "SELL ▼"
    else:
        icon, action = "⚪", signal

    now   = datetime.now().strftime("%H:%M  %d/%m/%Y")
    lines = [
        "<b>TÍN HIỆU RICH FOUNDATION</b>",
        f"{icon} <b>{action}</b> | <b>{ticker}</b>",
        f" ⏱ Khung: {format_interval(interval)}",
    ]

    if hit_price:
        if "TP3" in sig_upper:   label = "TP3"
        elif "TP2" in sig_upper: label = "TP2"
        elif "TP1" in sig_upper: label = "TP1"
        else:                    label = "SL"
        lines.append(f"💰 Giá chạm {label}: <b>{hit_price}</b>")
    else:
        lines.append(f"💰 Giá vào: <b>{close}</b>")
        if tp1: lines.append(f"🎯 TP1: {tp1}")
        if tp2: lines.append(f"🎯 TP2: {tp2}")
        if tp3: lines.append(f"🎯 TP3: {tp3}")
        if sl:  lines.append(f"🛑 SL:  {sl}")

    lines.append(f"🕐 {now}")
    return "\n".join(lines)


# --- ĐỊNH DẠNG TIN NHẮN TỔNG KẾT ---
def format_daily_summary() -> str:
    today      = datetime.now().strftime("%d/%m/%Y")
    total      = len(closed_trades) + len(open_trades)
    wins       = sum(1 for t in closed_trades if t["result"] in ("TP1", "TP2", "TP3"))
    losses     = sum(1 for t in closed_trades if t["result"] == "SL")
    still_open = len(open_trades)

    total_profit = sum(t["pnl"] for t in closed_trades if t["pnl"] and t["pnl"] > 0)
    total_loss   = sum(t["pnl"] for t in closed_trades if t["pnl"] and t["pnl"] < 0)
    net_pnl      = total_profit + total_loss
    win_rate     = f"{wins / len(closed_trades) * 100:.0f}%" if closed_trades else "—"
    net_icon     = "📈" if net_pnl >= 0 else "📉"
    net_sign     = "+" if net_pnl >= 0 else ""

    lines = [
        "🔔 <b>RICH FOUNDATION</b> 🔔",
        f"📊 <b>TỔNG KẾT NGÀY {today}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"📥 Tổng lệnh vào: <b>{total}</b>",
        f"✅ Thắng (TP): <b>{wins}</b>  |  ❌ Thua (SL): <b>{losses}</b>",
        f"📊 Tỉ lệ thắng: <b>{win_rate}</b>",
        "",
        f"💰 Tổng lãi:  +{total_profit:.2f}",
        f"📉 Tổng lỗ:   {total_loss:.2f}",
        f"{net_icon} Net P&L: <b>{net_sign}{net_pnl:.2f}</b>",
    ]

    if still_open:
        lines.append(f"⏳ Đang mở: <b>{still_open}</b> lệnh (chưa tính)")

    if closed_trades:
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("Chi tiết:")
        for t in closed_trades:
            icon  = "✅" if t["result"] == "TP" else "❌"
            sign  = "+" if t["pnl"] >= 0 else ""
            t_in  = datetime.fromisoformat(t["entry_time"]).strftime("%H:%M")
            t_out = datetime.fromisoformat(t["exit_time"]).strftime("%H:%M") if t["exit_time"] else "?"
            direction = "BUY" if t["direction"] == "LONG" else "SELL"
            lines.append(
                f"{icon} {direction} <b>{t['ticker']}</b>  "
                f"{sign}{t['pnl']:.2f}  |  {t_in}→{t_out}"
            )

    if open_trades:
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("⏳ Lệnh chưa đóng:")
        for ticker, t in open_trades.items():
            t_in      = datetime.fromisoformat(t["entry_time"]).strftime("%H:%M")
            direction = "BUY" if t["direction"] == "LONG" else "SELL"
            lines.append(f"  {direction} <b>{ticker}</b>  vào {t['entry_price']}  lúc {t_in}")

    return "\n".join(lines)


# --- GỬI TỚI TẤT CẢ NHÓM ---
async def send_all(message: str):
    for chat_id in cfg["dest_ids"]:
        try:
            await client.send_message(int(chat_id), message, parse_mode="html")
            print(f"  ✅ Gửi thành công -> {chat_id}")
        except Exception as e:
            print(f"  ❌ Lỗi tại {chat_id}: {type(e).__name__} — {e}")


# --- TÁC VỤ TỔNG KẾT HẰNG NGÀY ---
async def daily_summary_task():
    while True:
        now  = datetime.now()
        h, m = map(int, cfg["summary_time"].split(":"))
        target = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)

        wait = (target - now).total_seconds()
        print(f"📅 Tổng kết sẽ gửi lúc {cfg['summary_time']} (còn {wait / 3600:.1f}h)")
        await asyncio.sleep(wait)

        print("\n📊 Đang gửi tổng kết ngày...")
        await send_all(format_daily_summary())

        closed_trades.clear()
        open_trades.clear()
        if os.path.exists(TRADES_FILE):
            os.remove(TRADES_FILE)
        await asyncio.sleep(61)  # tránh gửi 2 lần trong cùng phút


# --- WEBHOOK ENDPOINT ---
@app.route("/webhook/<token>", methods=["POST"])
def webhook(token):
    if token != cfg["token"]:
        print(f"  ⛔ Token không hợp lệ: {token}")
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    signal    = data.get("signal", "unknown")
    sig_upper = signal.upper()
    print(f"\n📡 Nhận tín hiệu: {signal}  [{datetime.now().strftime('%H:%M:%S')}]")

    if "TP" in sig_upper or "SL" in sig_upper:
        record_trade_close(data)
    elif "LONG" in sig_upper or "SHORT" in sig_upper:
        record_trade_open(data)

    asyncio.run_coroutine_threadsafe(send_all(format_message(data)), loop)
    return jsonify({"status": "ok", "signal": signal}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":       "running",
        "open_trades":  len(open_trades),
        "closed_today": len(closed_trades),
    }), 200


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

    load_trades()
    await client.start()
    me = await client.get_me()

    print("\n" + "=" * 50)
    print(f"✅ Đăng nhập: {me.first_name}")
    print(f"🎯 Đích: {len(cfg['dest_ids'])} nhóm/kênh")
    print(f"🌐 Webhook: http://0.0.0.0:{cfg['port']}/webhook/{cfg['token']}")
    print(f"❤️  Health:  http://0.0.0.0:{cfg['port']}/health")
    print(f"📅 Tổng kết: {cfg['summary_time']} mỗi ngày")
    print("=" * 50 + "\n")

    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=cfg["port"], use_reloader=False),
        daemon=True,
    ).start()

    asyncio.ensure_future(daily_summary_task())
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng bot.")
