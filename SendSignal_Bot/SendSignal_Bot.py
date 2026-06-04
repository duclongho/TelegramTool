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

TRADES_FILE    = "trades.json"
YESTERDAY_FILE = "trades_yesterday.json"


# --- LƯU / TẢI DỮ LIỆU ---
def save_trades():
    data = {
        "date":          datetime.now().strftime("%Y-%m-%d"),
        "open_trades":   open_trades,
        "closed_trades": closed_trades,
    }
    with open(TRADES_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)


def save_yesterday():
    """Lưu dữ liệu ngày hiện tại vào file hôm qua trước khi xoá."""
    data = {
        "date":          datetime.now().strftime("%Y-%m-%d"),
        "closed_trades": list(closed_trades),
    }
    with open(YESTERDAY_FILE, "w", encoding="utf-8") as f:
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


def _parse_time(val) -> str:
    """Chuyển giá trị time từ Excel (có thể là '08:00', '08:00:00', datetime.time...) về 'HH:MM'."""
    import datetime as dt
    if isinstance(val, (dt.time, dt.datetime)):
        return f"{val.hour:02d}:{val.minute:02d}"
    parts = str(val).strip().split(":")
    return f"{int(parts[0]):02d}:{int(parts[1]):02d}"


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
            "summary_time": _parse_time(df["Giờ_Tổng_Kết"].iloc[0]) if "Giờ_Tổng_Kết" in df.columns else "23:59",
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
        tp4 = float(data.get("tp4", 0)) if data.get("tp4") else None
        tp5 = float(data.get("tp5", 0)) if data.get("tp5") else None
        sl  = float(data.get("sl",  0)) if data.get("sl")  else None
    except (ValueError, TypeError):
        tp1 = tp2 = tp3 = tp4 = tp5 = sl = None

    open_trades[ticker] = {
        "ticker":      ticker,
        "direction":   direction,
        "entry_price": entry_price,
        "tp1":         tp1,
        "tp2":         tp2,
        "tp3":         tp3,
        "tp4":         tp4,
        "tp5":         tp5,
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
    if "TP5" in sig_upper:
        result = "TP5"
    elif "TP4" in sig_upper:
        result = "TP4"
    elif "TP3" in sig_upper:
        result = "TP3"
    elif "TP2" in sig_upper:
        result = "TP2"
    elif "TP1" in sig_upper:
        result = "TP1"
    else:
        result = "SL"

    try:
        exit_price = float(data.get("hit_price", data.get("close", 0)))
    except (ValueError, TypeError):
        exit_price = 0.0

    # TP2–TP5: cập nhật record TP trước đó thay vì tạo bản ghi mới
    if result in ("TP2", "TP3", "TP4", "TP5"):
        for t in reversed(closed_trades):
            if t["ticker"] == ticker and t["result"] in ("TP1", "TP2", "TP3", "TP4"):
                pnl = abs(exit_price - t["entry_price"])
                t["exit_price"] = exit_price
                t["exit_time"]  = datetime.now().isoformat()
                t["result"]     = result
                t["pnl"]        = round(pnl, 2)
                save_trades()
                return

    trade = open_trades.pop(ticker, None)
    if not trade:
        return

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


# --- CHUYỂN ĐỔI GIÁ → PIP ---
def price_to_pips(ticker: str, price_diff: float) -> int:
    """Chuyển chênh lệch giá thành pip dựa theo loại instrument (có dấu)."""
    t = ticker.upper()
    if "XAU" in t:                                        # Vàng: 1 pip = 0.1
        pip_size = 0.1
    elif "XAG" in t:                                      # Bạc: 1 pip = 0.001
        pip_size = 0.001
    elif "JPY" in t:                                      # Cặp JPY: 1 pip = 0.01
        pip_size = 0.01
    elif any(c in t for c in ["BTC", "ETH", "BNB", "SOL", "XRP"]):  # Crypto: 1 pip = 1
        pip_size = 1.0
    else:                                                 # Forex thường: 1 pip = 0.0001
        pip_size = 0.0001
    sign = 1 if price_diff >= 0 else -1
    return int(round(abs(price_diff) / pip_size)) * sign


# --- KIỂM TRA GIÁ HỢP LỆ (loại bỏ NaN / rỗng từ Pine Script) ---
def _price(val) -> str | None:
    if not val:
        return None
    try:
        import math
        return None if math.isnan(float(val)) else str(val)
    except (ValueError, TypeError):
        return None


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
    tp1       = _price(data.get("tp1"))
    tp2       = _price(data.get("tp2"))
    tp3       = _price(data.get("tp3"))
    tp4       = _price(data.get("tp4"))
    tp5       = _price(data.get("tp5"))
    sl        = _price(data.get("sl"))
    hit_price = _price(data.get("hit_price"))

    sig_upper = signal.upper()
    if "LONG TP5" in sig_upper:
        icon, action = "🏆", "BUY TP5 ✅✅✅✅✅"
    elif "LONG TP4" in sig_upper:
        icon, action = "💎", "BUY TP4 ✅✅✅✅"
    elif "LONG TP3" in sig_upper:
        icon, action = "💰", "BUY TP3 ✅✅✅"
    elif "LONG TP2" in sig_upper:
        icon, action = "🎯", "BUY TP2 ✅✅"
    elif "LONG TP1" in sig_upper:
        icon, action = "✅", "BUY TP1 ✅"
    elif "LONG SL" in sig_upper:
        icon, action = "🛑", "BUY SL ❌"
    elif "SHORT TP5" in sig_upper:
        icon, action = "🏆", "SELL TP5 ✅✅✅✅✅"
    elif "SHORT TP4" in sig_upper:
        icon, action = "💎", "SELL TP4 ✅✅✅✅"
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
        if "TP5" in sig_upper:   label = "TP5"
        elif "TP4" in sig_upper: label = "TP4"
        elif "TP3" in sig_upper: label = "TP3"
        elif "TP2" in sig_upper: label = "TP2"
        elif "TP1" in sig_upper: label = "TP1"
        else:                    label = "SL"
        lines.append(f"💰 Giá chạm {label}: <b>{hit_price}</b>")
    else:
        lines.append(f"💰 Giá vào: <b>{close}</b>")
        if tp1: lines.append(f"🎯 TP1: {tp1}")
        if tp2: lines.append(f"🎯 TP2: {tp2}")
        if tp3: lines.append(f"🎯 TP3: {tp3}")
        if tp4: lines.append(f"🎯 TP4: {tp4}")
        if tp5: lines.append(f"🎯 TP5: {tp5}")
        if sl:  lines.append(f"🛑 SL:  {sl}")

    lines.append(f"🕐 {now}")
    return "\n".join(lines)


# --- ĐỊNH DẠNG TIN NHẮN TỔNG KẾT (dùng chung) ---
def _build_summary(title: str, trades: list, open_tds: dict | None = None) -> str:
    wins       = sum(1 for t in trades if t["result"] in ("TP1", "TP2", "TP3", "TP4", "TP5"))
    losses     = sum(1 for t in trades if t["result"] == "SL")
    still_open = len(open_tds) if open_tds else 0
    total      = len(trades) + still_open

    pip_profit = sum(price_to_pips(t["ticker"], t["pnl"]) for t in trades if t.get("pnl") and t["pnl"] > 0)
    pip_loss   = sum(price_to_pips(t["ticker"], t["pnl"]) for t in trades if t.get("pnl") and t["pnl"] < 0)
    net_pips   = pip_profit + pip_loss
    win_rate   = f"{wins / len(trades) * 100:.0f}%" if trades else "—"
    net_icon   = "📈" if net_pips >= 0 else "📉"
    net_sign   = "+" if net_pips >= 0 else ""

    lines = [
        "🔔 <b>RICH FOUNDATION</b> 🔔",
        f"📊 <b>{title}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"📥 Tổng lệnh: <b>{total}</b>",
        f"✅ Thắng (TP): <b>{wins}</b>  |  ❌ Thua (SL): <b>{losses}</b>",
        f"📊 Tỉ lệ thắng: <b>{win_rate}</b>",
        "",
        f"💰 Tổng lãi:  +{pip_profit} pip",
        f"📉 Tổng lỗ:   {pip_loss} pip",
        f"{net_icon} Net P&L: <b>{net_sign}{net_pips} pip</b>",
    ]

    if still_open:
        lines.append(f"⏳ Đang mở: <b>{still_open}</b> lệnh (chưa tính)")

    if trades:
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("Chi tiết:")
        for t in trades:
            icon      = "✅" if t["result"] in ("TP1", "TP2", "TP3", "TP4", "TP5") else "❌"
            pips      = price_to_pips(t["ticker"], t["pnl"])
            sign      = "+" if pips >= 0 else ""
            t_in      = datetime.fromisoformat(t["entry_time"]).strftime("%H:%M")
            t_out     = datetime.fromisoformat(t["exit_time"]).strftime("%H:%M") if t.get("exit_time") else "?"
            direction = "BUY" if t["direction"] == "LONG" else "SELL"
            lines.append(
                f"{icon} {direction} <b>{t['ticker']}</b> [{t['result']}]  "
                f"{sign}{pips} pip  |  {t_in}→{t_out}"
            )

    if open_tds:
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("⏳ Lệnh chưa đóng:")
        for ticker, t in open_tds.items():
            t_in      = datetime.fromisoformat(t["entry_time"]).strftime("%H:%M")
            direction = "BUY" if t["direction"] == "LONG" else "SELL"
            lines.append(f"  {direction} <b>{ticker}</b>  vào {t['entry_price']}  lúc {t_in}")

    return "\n".join(lines)


def format_daily_summary() -> str:
    today = datetime.now().strftime("%d/%m/%Y")
    return _build_summary(f"TỔNG KẾT NGÀY {today}", list(closed_trades), dict(open_trades))


def format_morning_summary() -> str | None:
    """Tổng kết ngày hôm qua — gửi đầu buổi sáng."""
    if not os.path.exists(YESTERDAY_FILE):
        return None
    try:
        with open(YESTERDAY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        trades = data.get("closed_trades", [])
        raw_date = data.get("date", "")
        try:
            label_date = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%d/%m/%Y")
        except ValueError:
            label_date = raw_date
    except Exception:
        return None
    if not trades:
        return None
    return _build_summary(f"TỔNG KẾT NGÀY {label_date}", trades)


def format_partial_summary(from_h: int, to_h: int, period_name: str) -> str:
    """Tổng kết theo khung giờ trong ngày (lọc theo giờ đóng lệnh)."""
    trades = [
        t for t in closed_trades
        if t.get("exit_time")
        and from_h <= datetime.fromisoformat(t["exit_time"]).hour < to_h
    ]
    today = datetime.now().strftime("%d/%m/%Y")
    return _build_summary(f"THỐNG KÊ {period_name} {today}", trades)


# --- GỬI TỚI TẤT CẢ NHÓM ---
async def send_all(message: str):
    for chat_id in cfg["dest_ids"]:
        try:
            await client.send_message(int(chat_id), message, parse_mode="html")
            print(f"  ✅ Gửi thành công -> {chat_id}")
        except Exception as e:
            print(f"  ❌ Lỗi tại {chat_id}: {type(e).__name__} — {e}")


# --- LỊCH GỬI THÔNG BÁO ---
# (hour, minute, type, *args)
#   "morning" : ĐỌC file hôm qua → gửi (không lưu, không reset)
#   "partial" : thống kê khung giờ trong ngày
#   "reset"   : tổng kết cuối ngày + lưu hôm nay làm hôm qua + reset
#
# Giờ_Tổng_Kết (Excel) = giờ sáng hiển thị tổng kết hôm qua
# Reset cố định lúc 23:55 mỗi ngày
_FIXED_SCHEDULE = [
    (12, 0,  "partial", 0,  12, "BUỔI SÁNG"),
    (17, 0,  "partial", 12, 17, "BUỔI CHIỀU"),
    (22, 0,  "partial", 17, 22, "BUỔI TỐI"),
    (23, 55, "reset"),
]


async def notification_scheduler():
    while True:
        now = datetime.now()
        h_m, m_m = map(int, cfg["summary_time"].split(":"))

        # Giờ sáng từ Excel + lịch cố định
        schedule = list(_FIXED_SCHEDULE) + [(h_m, m_m, "morning")]

        # Tìm thông báo kế tiếp gần nhất
        candidates = []
        for item in schedule:
            h, m = item[0], item[1]
            target = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            candidates.append((target, item))
        candidates.sort(key=lambda x: x[0])

        next_time, next_item = candidates[0]
        ntype = next_item[2]
        wait  = (next_time - now).total_seconds()
        print(f"⏰ Thông báo tiếp theo: [{ntype}] lúc {next_time.strftime('%H:%M %d/%m')} (còn {wait / 3600:.1f}h)")

        await asyncio.sleep(wait)

        # Thứ 7 (5) và Chủ Nhật (6): bỏ qua tất cả thông báo
        if datetime.now().weekday() >= 5:
            print(f"📅 Cuối tuần — bỏ qua thông báo [{ntype}]")
            await asyncio.sleep(61)
            continue

        if ntype == "morning":
            # Chỉ ĐỌC file hôm qua (đã lưu từ 23:55 đêm qua) → gửi
            msg = format_morning_summary()
            if msg:
                print("\n🌅 Đang gửi tổng kết hôm qua...")
                await send_all(msg)
            else:
                print("\n🌅 Không có dữ liệu hôm qua, bỏ qua.")

        elif ntype == "partial":
            _, _, _, from_h, to_h, period_name = next_item
            print(f"\n📊 Đang gửi thống kê {period_name}...")
            await send_all(format_partial_summary(from_h, to_h, period_name))

        elif ntype == "reset":
            # 23:55: lưu hôm nay làm hôm qua + reset (không gửi thông báo, sáng mai sẽ gửi)
            print("\n🔄 Reset cuối ngày...")
            save_yesterday()
            closed_trades.clear()
            open_trades.clear()
            if os.path.exists(TRADES_FILE):
                os.remove(TRADES_FILE)

        await asyncio.sleep(61)  # tránh kích hoạt 2 lần trong cùng phút


# --- WEBHOOK ENDPOINT ---
@app.route("/webhook/<token>", methods=["POST"])
def webhook(token):
    if token != cfg["token"]:
        print(f"  ⛔ Token không hợp lệ: {token}")
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(force=True, silent=True)
    if not data:
        raw = request.get_data(as_text=True)
        print(f"  ⚠️ JSON lỗi. Body nhận được: {repr(raw[:500])}")
        return jsonify({"error": "Invalid JSON", "received": raw[:200]}), 400

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
    print(f"📅 Lịch thông báo: {cfg['summary_time']} (hôm qua + reset) | 12:00 | 17:00 | 22:00")
    print("=" * 50 + "\n")

    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=cfg["port"], use_reloader=False),
        daemon=True,
    ).start()

    asyncio.ensure_future(notification_scheduler())
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng bot.")
