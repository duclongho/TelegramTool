# SendSignal_Bot

## Tổng quan

Bot nhận tín hiệu **Buy / Sell** từ indicator **UT Bot Alerts** trên TradingView qua webhook,
rồi gửi tin nhắn định dạng đẹp tới nhiều nhóm/kênh Telegram bằng tài khoản cá nhân (Telethon).

```
TradingView Alert (webhook POST)
    → Flask server  /webhook
    → Telethon
    → Telegram groups
```

## Công nghệ

- **Python** 3.12+
- **Telethon** — gửi tin qua tài khoản Telegram cá nhân
- **Flask** — nhận webhook từ TradingView
- **pandas / openpyxl** — đọc cấu hình từ Excel

## Cấu trúc project

```
SendSignal_Bot/
├── CLAUDE.md
├── SendSignal_Bot.py   # Flask server + Telethon client
├── UTBotAlerts.py      # Pine Script indicator (tham khảo)
├── Data.xlsx           # Cấu hình (API, danh sách nhóm, port)
├── requirements.txt
└── RUN.bat             # Khởi động
```

## Cấu hình (Data.xlsx)

| Cột | Ví dụ | Ý nghĩa |
|-----|-------|---------|
| `Mã_API` | `12345678` | Telegram API ID |
| `Chuỗi_API` | `abc123...` | Telegram API Hash |
| `Danh_Sách_ID_Nhận` | `-1001234567890, -1009876543210` | Chat ID các nhóm/kênh, phân cách bằng dấu phẩy |
| `Port` | `5000` | Port Flask lắng nghe webhook (tuỳ chọn, mặc định 5000) |

> Lấy API ID & Hash tại: https://my.telegram.org/apps

## Cách chạy

```bat
RUN.bat
```

**Chạy nền (không tắt khi đóng cửa sổ) — dùng Task Scheduler hoặc NSSM:**
```bat
:: Cài NSSM rồi chạy lệnh này một lần để đăng ký service
nssm install SendSignalBot "python" "C:\path\to\SendSignal_Bot.py"
nssm start SendSignalBot
```

## Cài đặt TradingView Alert

### 1. Thêm indicator UT Bot Alerts vào chart

### 2. Tạo Alert cho tín hiệu BUY
- **Condition:** `UT Long`
- **Webhook URL:** `http://<IP_VPS>:5000/webhook`
- **Message:**
```json
{"signal":"UT Long","ticker":"{{ticker}}","close":"{{close}}","interval":"{{interval}}","exchange":"{{exchange}}"}
```

### 3. Tạo Alert cho tín hiệu SELL
- **Condition:** `UT Short`
- **Webhook URL:** `http://<IP_VPS>:5000/webhook`
- **Message:**
```json
{"signal":"UT Short","ticker":"{{ticker}}","close":"{{close}}","interval":"{{interval}}","exchange":"{{exchange}}"}
```

## Định dạng tin nhắn gửi về Telegram

**BUY:**
```
🟢 UT LONG ▲ | BTCUSDT.P
📊 Sàn: BYBIT  |  ⏱ Khung: 1h
💰 Giá đóng cửa: 65432.5
🕐 15:30  18/05/2026
```

**SELL:**
```
🔴 UT SHORT ▼ | BTCUSDT.P
📊 Sàn: BYBIT  |  ⏱ Khung: 1h
💰 Giá đóng cửa: 65100.0
🕐 15:45  18/05/2026
```

## Endpoints

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `/webhook` | POST | Nhận tín hiệu từ TradingView |
| `/health` | GET | Kiểm tra bot còn sống không |

## Lưu ý

- Mở port trên VPS Windows: vào **Windows Defender Firewall → Inbound Rules → New Rule → Port 5000**.
- File `signal_session.session` được tạo sau lần đăng nhập đầu tiên — không xoá, không commit.
- `Data.xlsx` chứa thông tin nhạy cảm — không commit lên git.
- TradingView gửi webhook theo múi giờ UTC; bot hiển thị giờ theo máy chủ.
