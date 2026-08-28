#!/usr/bin/env python3
"""
Binance Futures Song Kiem Signal Bot
Chỉnh TELEGRAM_TOKEN, TELEGRAM_CHAT_ID trước khi chạy.
"""
import asyncio
import json
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Literal, TypedDict

import aiohttp

# ═══════════════════════════════════════════════
#  CẤU HÌNH — chỉnh ở đây
# ═══════════════════════════════════════════════
TELEGRAM_TOKEN         = "8641278115:AAEB08VXrX5YJl_2zzM_SFF4JRdEwIfAj-s"   # Token bot Telegram
TELEGRAM_CHAT_ID       = "-1004448248877"   # Chat ID nhận kèo BB H1 Rút Râu (LONG SIGNAL (H1) + SHORT SIGNAL (H1))
TELEGRAM_CHAT_ID_H1    = "-1004340326145"   # Chat ID nhận kèo RSI H4 Đảo Biên (LONG/SHORT SIGNAL H4, intrabar)
TELEGRAM_CHAT_ID_CHANNEL = "-1003575342337"   # Chat ID nhận kèo Kênh Song Song 3 Điểm (chạm Line B lần 2 -> đảo chiều)

# Tên gọi CHÍNH THỨC của 3 kèo — dùng thống nhất trong tin nhắn Telegram + thống kê cuối ngày.
# (Kèo BB H1 Đột Biến và BB RSI H1 — 2 kèo AUTO-TRADE tiền thật qua executor.py — đã bị XÓA
# khỏi bot, cùng với executor.py, theo yêu cầu chỉ giữ lại các kèo tín hiệu bên dưới.)
KEO_RUTRAU_NAME   = "BB H1 Rút Râu"          # <-> TELEGRAM_CHAT_ID       (nến rút râu chạm BB trên/dưới)
KEO_LEGACY_NAME   = "RSI H4 Đảo Biên"        # <-> TELEGRAM_CHAT_ID_H1    (RSI6 vượt rồi quay đầu qua mốc 10/90, intrabar)
KEO_CHANNEL_NAME  = "Kênh Song Song 3 Điểm"  # <-> TELEGRAM_CHAT_ID_CHANNEL (3 điểm xoay -> kênh song song -> đảo chiều)

AUTO_TOP_SYMBOLS  = True   # True = tự động lấy top coin theo khối lượng
TOP_SYMBOLS_COUNT = 200     # Số lượng coin theo dõi (LONG-H1 + SHORT-H1 mới)
LEGACY_TOP_SYMBOLS_COUNT = 150   # Số lượng coin theo dõi riêng cho kèo RSI H4 Đảo Biên (feed H4 riêng)

SYMBOLS = [                # Dùng khi AUTO_TOP_SYMBOLS = False
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

CANDLE_BUFFER = 150

INTERVAL_H1         = "1h"   # Timeframe dùng chung cho hầu hết các kèo
INTERVAL_H1_DISPLAY = "H1"

INTERVAL_H4         = "4h"   # Timeframe riêng cho kèo RSI H4 Đảo Biên (feed WebSocket riêng)
INTERVAL_H4_DISPLAY = "H4"

BB_PERIOD = 20
BB_STD    = 2.0

DOJI_BODY_MAX_RATIO       = 0.3   # Thân nến tối đa 30% tổng biên độ nến (high-low) — coi là nến doji
DOJI_SHORT_WICK_MAX_RATIO = 0.1   # Râu phía đối diện hướng đảo chiều tối đa 10% tổng biên độ (gần như không có)
BAND_CROSS_MIN_RATIO      = 0.1   # Phần xuyên qua BB trên/dưới tối thiểu 10% tổng biên độ nến

MIN_CANDLES_FOR_SIGNAL = BB_PERIOD + 5  # Số nến tối thiểu cần có trước khi bắt đầu xét tín hiệu

ALERT_COOLDOWN_MINUTES        = 30    # Cooldown giữa 2 tín hiệu cùng coin/chiều
LEGACY_ALERT_COOLDOWN_MINUTES = 8 * 60  # Cooldown riêng cho kèo RSI H4 Đảo Biên: 8 tiếng/cặp kể từ lúc VÀO
                                     # lệnh, tính CHUNG cho cả 2 chiều (không tách LONG/SHORT) — tránh báo
                                     # liên tục trên cùng 1 cặp ngay sau khi vừa thông báo vào lệnh

DOJI_TP_PCT = 0.02     # Chốt lời cố định 2% (kèo BB H1 Rút Râu)
DOJI_SL_PCT = 0.015    # Cắt lỗ cố định 1.5% (kèo BB H1 Rút Râu)

# Kèo RSI H4 Đảo Biên — INTRABAR (không chờ đóng nến, theo dõi RSI6 liên tục trong cây H4
# đang hình thành): SHORT khi RSI đã vượt LÊN trên 90 (armed) rồi quay lại lùi xuống tới 85
# (đảo chiều từ quá mua); LONG khi RSI đã vượt XUỐNG dưới 10 (armed) rồi quay lại tăng lên tới
# 15 (đảo chiều từ quá bán). Mốc armed (90/10) và mốc xác nhận bắn (85/15) TÁCH RIÊNG — dùng
# khoảng đệm 5 điểm RSI để lọc bớt tín hiệu nhiễu khi RSI chỉ vừa chớm qua lại sát biên. Trạng
# thái armed chỉ tính TRONG PHẠM VI 1 cây H4 đang chạy — tự reset mỗi khi có nến H4 mới mở,
# không cộng dồn qua nhiều cây. Chỉ báo Telegram, KHÔNG tự đặt lệnh thật (không dùng executor)
# — xem RsiExtremeScanner.
LEGACY_RSI_PERIOD       = 6      # Chu kỳ RSI (đồng bộ với kèo BB RSI H1)
LEGACY_RSI_OVERBOUGHT   = 90     # SHORT: RSI vượt lên trên mốc này thì armed
LEGACY_RSI_SHORT_CONFIRM = 85    # SHORT: armed rồi RSI lùi về tới mốc này (<=) thì bắn
LEGACY_RSI_OVERSOLD     = 10     # LONG: RSI vượt xuống dưới mốc này thì armed
LEGACY_RSI_LONG_CONFIRM = 15     # LONG: armed rồi RSI tăng lên tới mốc này (>=) thì bắn
LEGACY_TP_PCT = 0.04    # Chốt lời cố định 4% (kèo RSI H4 Đảo Biên)
LEGACY_SL_PCT = 0.03    # Cắt lỗ cố định 3% (kèo RSI H4 Đảo Biên)

# Kèo Kênh Song Song 3 Điểm — xem thiết kế đầy đủ đã thống nhất (bản minh hoạ trực quan):
# lấy 3 điểm xoay (đỉnh/đáy fractal) gần nhất, luôn xen kẽ loại (Đáy-Đỉnh-Đáy hoặc
# Đỉnh-Đáy-Đỉnh). 2 điểm CÙNG loại dựng "Line A" (đường gốc, độ dốc kênh); điểm CÒN LẠI
# dựng "Line B" song song Line A. Vào lệnh ĐẢO CHIỀU khi giá quay lại CHẠM Line B lần 2
# (không phải phá vỡ): Line B là biên TRÊN (dựng từ 2 đáy + 1 đỉnh) -> chạm -> SHORT; Line B
# là biên DƯỚI (dựng từ 2 đỉnh + 1 đáy) -> chạm -> LONG. Chỉ báo Telegram — KHÔNG tự đặt lệnh
# thật (không nhận executor, giống 2 kèo còn lại). Xem ChannelScanner.
CHANNEL_FRACTAL_K = 3     # Số nến xác nhận mỗi bên cho 1 điểm xoay (càng lớn càng ít nhiễu,
                           # nhưng càng trễ xác nhận) — 1 nến chỉ được coi là đỉnh/đáy sau khi
                           # đã có đủ K nến ĐÓNG CỬA ở CẢ 2 BÊN xác nhận nó là cực trị cục bộ.
CHANNEL_SL_PCT           = 0.02   # Cắt lỗ cố định 2% từ entry — KHÔNG bám theo Line B
CHANNEL_TP1_PCT          = 0.03   # Chốt 1 phần tại 3% từ entry
CHANNEL_TP2_PCT          = 0.05   # Chốt nốt phần còn lại tại 5% từ entry
CHANNEL_TP1_CLOSE_RATIO  = 0.5    # Tỉ lệ khối lượng chốt tại TP1 (50%) — chỉ để HIỂN THỊ trong
                                    # tin Telegram (kèo này không tự đặt lệnh thật nên không có
                                    # khối lượng thật để chốt, chỉ báo tín hiệu quản lý cho người
                                    # tự thao tác tay).

WS_MAX_STREAMS_PER_CONN = 190   # Giới hạn an toàn số stream / 1 kết nối WebSocket (Binance giới hạn ~200)
WS_RECONNECT_DELAY_SEC  = 5     # Chờ trước khi kết nối lại sau khi WebSocket bị rớt
WS_HEARTBEAT_MINUTES    = 60    # Log xác nhận vẫn đang kết nối Binance mỗi N phút
WS_NO_DATA_TIMEOUT_SEC  = 60    # Nếu không nhận được bất kỳ message nào trong N giây -> coi là treo, kết nối lại

# ═══════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
#  INDICATORS
# ═══════════════════════════════════════════════
class Indicators(TypedDict):
    bb_upper:  float
    bb_middle: float
    bb_lower:  float


def _calc_bb(closes: list[float], period: int, multiplier: float) -> tuple[float, float, float]:
    if len(closes) < period:
        return 0.0, 0.0, 0.0
    window   = closes[-period:]
    middle   = sum(window) / period
    variance = sum((x - middle) ** 2 for x in window) / period  # population std
    std      = variance ** 0.5
    return middle + multiplier * std, middle, middle - multiplier * std


def compute_indicators(candles: list[dict]) -> Indicators:
    closes = [c["close"] for c in candles]
    bb_upper, bb_middle, bb_lower = _calc_bb(closes, BB_PERIOD, BB_STD)
    return Indicators(bb_upper=bb_upper, bb_middle=bb_middle, bb_lower=bb_lower)


def _calc_rsi(closes: list[float], period: int) -> list[float]:
    """RSI kiểu Wilder (smoothing), trả về 1 list RSI cùng độ dài với closes (các vị trí chưa đủ
    dữ liệu = 50.0 — trung tính). Dùng để lấy 2 giá trị gần nhất (trước/hiện tại) phát hiện CẮT
    NGƯỠNG, giống cách BB dùng cho phát hiện xuyên biên."""
    n = len(closes)
    if n < period + 1:
        return [50.0] * n

    rsis = [50.0] * n
    gains = losses = 0.0
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains += max(diff, 0.0)
        losses += max(-diff, 0.0)
    avg_gain = gains / period
    avg_loss = losses / period
    rsis[period] = 100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    for i in range(period + 1, n):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0.0)
        loss = max(-diff, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rsis[i] = 100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    return rsis


class SwingPoint(TypedDict):
    bar_open: int      # mốc mở nến (ms) của điểm xoay — dùng làm trục thời gian TUYỆT ĐỐI
                         # thay vì vị trí trong list (list là 1 deque maxlen=CANDLE_BUFFER, vị
                         # trí phần tử SẼ đổi theo thời gian khi nến cũ bị đẩy ra -> không thể
                         # dùng index làm trục ổn định cho phương trình đường thẳng lâu dài).
    price:    float     # giá cao/thấp tại điểm xoay
    type:     Literal["H", "L"]   # "H" = đỉnh (swing high), "L" = đáy (swing low)


def _find_swing_points(candles: list[dict], k: int, count: int = 3) -> list[SwingPoint] | None:
    """Quét TOÀN BỘ candles theo kiểu zigzag, giữ lại chuỗi đỉnh/đáy XEN KẼ NGHIÊM NGẶT — nếu
    gặp 1 đỉnh mới CAO HƠN đỉnh đang giữ (chưa có đáy nào xen giữa) thì THAY THẾ đỉnh cũ (đỉnh
    cũ chỉ là điểm trung gian, không phải điểm xoay thật), tương tự cho đáy. Chỉ xét nến đã
    XÁC NHẬN (còn đủ k nến ĐÃ ĐÓNG ở cả 2 bên — nến cuối cùng trong `candles` có thể vẫn đang
    hình thành nên KHÔNG được coi là đã xác nhận). Trả về `count` điểm gần nhất (cũ -> mới),
    None nếu chưa đủ dữ liệu/chưa đủ điểm xoay."""
    n = len(candles)
    last_confirmable = n - 1 - k
    if last_confirmable < k:
        return None

    zigzag: list[SwingPoint] = []
    for idx in range(k, last_confirmable + 1):
        bar_open = candles[idx].get("bar_open")
        if bar_open is None:
            continue   # nến nạp từ REST lúc mới khởi động thiếu bar_open -> bỏ qua, an toàn
        before = candles[idx - k:idx]
        after  = candles[idx + 1:idx + 1 + k]
        hi, lo = candles[idx]["high"], candles[idx]["low"]
        is_high = all(hi > c["high"] for c in before) and all(hi > c["high"] for c in after)
        is_low  = all(lo < c["low"]  for c in before) and all(lo < c["low"]  for c in after)
        if not (is_high or is_low):
            continue
        # Hiếm khi 1 nến vừa là đỉnh vừa là đáy cục bộ (biên độ rất lớn) -> ưu tiên đỉnh, đơn
        # giản hoá (không ảnh hưởng nhiều vì đây là biến động bất thường, hiếm gặp).
        ptype = "H" if is_high else "L"
        price = hi if is_high else lo
        point: SwingPoint = {"bar_open": bar_open, "price": price, "type": ptype}

        if zigzag and zigzag[-1]["type"] == ptype:
            more_extreme = (price > zigzag[-1]["price"]) if ptype == "H" else (price < zigzag[-1]["price"])
            if more_extreme:
                zigzag[-1] = point   # điểm cũ chỉ là trung gian, thay bằng điểm cực trị hơn
        else:
            zigzag.append(point)

    if len(zigzag) < count:
        return None
    return zigzag[-count:]


class Channel(TypedDict):
    slope:      float    # giá / mili-giây — độ dốc CHUNG cho cả Line A và Line B (song song)
    lineA_at:   Callable[[int], float]   # giá trị Line A tại 1 mốc bar_open bất kỳ
    lineB_at:   Callable[[int], float]   # giá trị Line B tại 1 mốc bar_open bất kỳ
    direction:  Literal["LONG", "SHORT"]   # hướng vào lệnh khi giá CHẠM Line B lần 2
    p3_bar_open: int     # bar_open của điểm xoay MỚI NHẤT dùng dựng kênh — để phát hiện kênh
                           # đã đổi (có điểm xoay mới xác nhận) hay vẫn là kênh cũ


def _build_channel(points: list[SwingPoint]) -> Channel | None:
    """Dựng kênh song song từ 3 điểm xoay (points[0] cũ nhất -> points[2] mới nhất, đã xen kẽ
    loại — xem _find_swing_points). Line A nối 2 điểm CÙNG loại (p1, p3); Line B song song
    Line A, đi qua điểm CÒN LẠI (p2). p2 là "Đỉnh" -> Line B là biên TRÊN -> chạm -> SHORT;
    p2 là "Đáy" -> Line B là biên DƯỚI -> chạm -> LONG."""
    p1, p2, p3 = points
    if p1["type"] != p3["type"] or p1["type"] == p2["type"]:
        return None   # không đúng dạng xen kẽ Đáy-Đỉnh-Đáy / Đỉnh-Đáy-Đỉnh
    dt = p3["bar_open"] - p1["bar_open"]
    if dt <= 0:
        return None
    slope = (p3["price"] - p1["price"]) / dt

    def lineA_at(bar_open: int) -> float:
        return p1["price"] + slope * (bar_open - p1["bar_open"])

    def lineB_at(bar_open: int) -> float:
        return p2["price"] + slope * (bar_open - p2["bar_open"])

    direction: Literal["LONG", "SHORT"] = "SHORT" if p2["type"] == "H" else "LONG"
    return Channel(slope=slope, lineA_at=lineA_at, lineB_at=lineB_at,
                   direction=direction, p3_bar_open=p3["bar_open"])


# ═══════════════════════════════════════════════
#  SIGNAL
# ═══════════════════════════════════════════════
@dataclass
class Signal:
    symbol:    str
    direction: Literal["LONG", "SHORT"]
    price:     float
    sl:        float
    ind:       Indicators


@dataclass
class Position:
    """Lệnh đang mở, chờ chạm TP hoặc SL."""
    symbol:    str
    direction: Literal["LONG", "SHORT"]
    entry:     float
    tp:        float
    sl:        float
    opened_at: datetime
    entry_bar_open: int | None = None   # bar_open của nến lúc tín hiệu phát ra (kèo RSI H4 Đảo Biên
                                          # dùng) — để tránh tính TP/SL lùi về biên độ đã có TRƯỚC lúc
                                          # vào lệnh (xem _position_hit)


def _position_hit(pos: Position, candle: dict) -> Literal["TP", "SL"] | None:
    """Kiểm tra 1 nến (đã đóng hoặc đang hình thành) có chạm TP/SL của lệnh đang mở không.

    Nếu candle CHÍNH LÀ nến lúc tín hiệu vừa phát ra (entry_bar_open trùng bar_open của
    nến) thì dùng giá ĐÓNG CỬA hiện tại thay vì cao/thấp của cả nến — vì nến đột biến vốn
    đã có biên độ lớn TRƯỚC khi đủ điều kiện báo tín hiệu, dùng high/low cả nến sẽ tính
    lùi luôn phần biến động đã xảy ra trước lúc vào lệnh (khiến TP/SL báo "chạm" gần như
    ngay lập tức dù chưa có biến động mới nào sau khi vào lệnh)."""
    is_entry_bar = pos.entry_bar_open is not None and candle.get("bar_open") == pos.entry_bar_open

    if is_entry_bar:
        price = candle["close"]
        if pos.direction == "SHORT":
            hit_tp = price <= pos.tp
            hit_sl = price >= pos.sl
        else:
            hit_tp = price >= pos.tp
            hit_sl = price <= pos.sl
    elif pos.direction == "SHORT":
        hit_tp = candle["low"]  <= pos.tp
        hit_sl = candle["high"] >= pos.sl
    else:
        hit_tp = candle["high"] >= pos.tp
        hit_sl = candle["low"]  <= pos.sl

    if not (hit_tp or hit_sl):
        return None

    if hit_tp and hit_sl:
        # Cả 2 mốc bị chạm trong cùng 1 nến — ước lượng theo hướng nến để chọn mốc chạm trước
        bearish = candle["close"] <= candle["open"]
        return ("TP" if bearish else "SL") if pos.direction == "SHORT" else ("SL" if bearish else "TP")
    return "TP" if hit_tp else "SL"


@dataclass
class ChannelPosition:
    """Lệnh đang theo dõi của kèo Kênh Song Song 3 Điểm — riêng dataclass (không dùng chung
    Position) vì có 2 mốc chốt lời (TP1/TP2) thay vì 1, và SL có thể ĐỔI (dời về entry sau khi
    chạm TP1) — 2 điều 3 kèo còn lại không cần."""
    symbol:    str
    direction: Literal["LONG", "SHORT"]
    entry:     float
    sl:        float    # ban đầu = entry ± CHANNEL_SL_PCT, ĐỔI thành đúng giá entry sau khi chạm TP1
    tp1:       float
    tp2:       float
    opened_at: datetime
    tp1_hit:   bool = False   # đã chốt 1 phần tại TP1 chưa (đang chờ TP2 hoặc SL breakeven)


def _channel_position_hit(pos: ChannelPosition, candle: dict) -> Literal["TP1", "TP2", "SL"] | None:
    """Kiểm tra 1 nến có chạm SL / TP1 (nếu CHƯA chốt) / TP2 (nếu ĐÃ chốt TP1) không. Nhiều
    mốc cùng chạm trong 1 nến -> ước lượng theo hướng nến, giống _position_hit gốc."""
    if pos.direction == "SHORT":
        hit_sl  = candle["high"] >= pos.sl
        hit_tp1 = (not pos.tp1_hit) and candle["low"] <= pos.tp1
        hit_tp2 = pos.tp1_hit and candle["low"] <= pos.tp2
    else:
        hit_sl  = candle["low"] <= pos.sl
        hit_tp1 = (not pos.tp1_hit) and candle["high"] >= pos.tp1
        hit_tp2 = pos.tp1_hit and candle["high"] >= pos.tp2

    hits = {name for name, hit in (("SL", hit_sl), ("TP1", hit_tp1), ("TP2", hit_tp2)) if hit}
    if not hits:
        return None
    if len(hits) == 1:
        return hits.pop()

    # Cả SL lẫn TP (TP1 hoặc TP2) cùng chạm trong 1 nến — ước lượng theo hướng nến để chọn mốc
    # chạm trước, giống _position_hit gốc.
    bearish  = candle["close"] <= candle["open"]
    favorable = bearish if pos.direction == "SHORT" else not bearish
    if favorable:
        return "TP2" if "TP2" in hits else "TP1"
    return "SL"


class DailyStats:
    """Đếm tín hiệu/kết quả TRONG NGÀY cho 1 kèo — dùng CHUNG cho cả 2 Scanner chiều
    Long/Short của cùng 1 kèo (vd long_scanner + short_scanner cùng ghi vào 1 DailyStats
    "H1 mới"), vì đây là ước lượng theo nến (không phải PnL thật) nên chỉ đếm % thắng/thua,
    không tính USDT."""

    def __init__(self, name: str, chat_id: str) -> None:
        self.name    = name
        self.chat_id = chat_id
        self.total   = 0
        self.wins    = 0
        self.losses  = 0

    def record_open(self) -> None:
        self.total += 1

    def record_result(self, hit: Literal["TP", "SL"]) -> None:
        if hit == "TP":
            self.wins += 1
        else:
            self.losses += 1

    def reset(self) -> None:
        self.total = 0
        self.wins = 0
        self.losses = 0

    def build_message(self, date_label: str) -> str:
        closed     = self.wins + self.losses
        win_rate   = f"{self.wins / closed * 100:.0f}%" if closed else "—"
        still_open = self.total - closed
        lines = [
            f"📊 *TỔNG KẾT NGÀY — {date_label}*",
            "━━━━━━━━━━━━━━━━━━━━━━",
            f"📥 Tổng tín hiệu: *{self.total}*",
            f"✅ Thắng (TP): *{self.wins}*  |  ❌ Thua (SL): *{self.losses}*",
            f"📊 Tỉ lệ thắng: *{win_rate}*",
        ]
        if still_open > 0:
            lines.append(f"⏳ Đang mở/chưa rõ kết quả: *{still_open}*")
        return "\n".join(lines)


def detect_signal(symbol: str, candles: list[dict],
                   direction: Literal["LONG", "SHORT"] = "LONG") -> Signal | None:
    if len(candles) < BB_PERIOD + 2:
        return None

    prev = candles[-2]   # Nến ngay trước — dùng để so khối lượng
    curr = candles[-1]   # Nến chuồn chuồn/bia mộ chạm band — báo tín hiệu ngay khi đóng cửa

    rng = curr["high"] - curr["low"]
    if rng <= 0:
        return None

    body = abs(curr["close"] - curr["open"])
    if body > DOJI_BODY_MAX_RATIO * rng:
        return None   # Thân nến quá lớn, không phải doji

    if curr["volume"] < 2 * prev["volume"]:
        return None   # Khối lượng phải gấp đôi trở lên nến trước

    # BB tính đến trước nến hiện tại, tránh self-reference
    ind = compute_indicators(candles[:-1])
    if ind["bb_middle"] == 0.0:
        return None

    if direction == "LONG":
        # Nến rút râu chuyển hẳn sang xanh: chạm/xuyên sâu BB dưới rồi bật lên đóng cửa xanh
        if not (curr["close"] > curr["open"]):
            return None   # Đóng nến đỏ -> bỏ qua
        upper_wick = curr["high"] - max(curr["open"], curr["close"])
        if upper_wick > DOJI_SHORT_WICK_MAX_RATIO * rng:
            return None
        if ind["bb_lower"] - curr["low"] < BAND_CROSS_MIN_RATIO * rng:
            return None
        if curr["high"] >= ind["bb_middle"]:
            return None
    else:
        # Nến rút râu chuyển hẳn sang đỏ: chạm/xuyên sâu BB trên rồi rớt xuống đóng cửa đỏ
        if not (curr["close"] < curr["open"]):
            return None   # Đóng nến xanh -> bỏ qua
        lower_wick = min(curr["open"], curr["close"]) - curr["low"]
        if lower_wick > DOJI_SHORT_WICK_MAX_RATIO * rng:
            return None
        if curr["high"] - ind["bb_upper"] < BAND_CROSS_MIN_RATIO * rng:
            return None
        if curr["low"] <= ind["bb_middle"]:
            return None

    logger.info(f"{symbol} {direction} | Entry={curr['close']:.4f}")
    return Signal(symbol=symbol, direction=direction, price=curr["close"], sl=0.0, ind=ind)


# detect_legacy_signal() (BB vượt biên, momentum) đã được thay bằng kèo RSI H4 Đảo Biên —
# logic INTRABAR có trạng thái (armed/reset theo từng cây H4) nên nằm trực tiếp trong
# RsiExtremeScanner thay vì 1 hàm detect_fn thuần tuý như các kèo khác — xem class đó.


# detect_spike_signal() (kèo BB H1 Đột Biến) và detect_midcross_signal() (kèo BB RSI H1) đã bị
# XÓA cùng với SpikeScanner/MidCrossScanner và executor.py — 2 kèo đó KHÔNG còn chạy trong bot.

# ═══════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════
def _fmt(price: float) -> str:
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


def _build_message(signal: Signal, interval_display: str, tp: float) -> str:
    is_short   = signal.direction == "SHORT"
    emoji      = "🔴" if is_short else "🟢"
    band       = "trên" if is_short else "dưới"
    candle_dir = "cây nến giảm" if is_short else "cây nến tăng"
    return (
        f"*{emoji} {signal.direction} SIGNAL - {interval_display}*\n\n"
        f"Coin: `{signal.symbol}`\n\n"
        f"Điều kiện:\n"
        f"✓ Nến rút râu xuyên qua BB {band}\n"
        f"✓ Là {candle_dir}\n"
        f"✓ Chưa vượt qua BB giữa\n"
        f"✓ Khối lượng gấp đôi trở lên nến trước\n\n"
        f"Entry: `{_fmt(signal.price)}`\n"
        f"TP: `{_fmt(tp)}`\n"
        f"SL: `{_fmt(signal.sl)}`"
    )


def _build_h4_rsi_message(signal: Signal, interval_display: str, tp: float) -> str:
    is_short     = signal.direction == "SHORT"
    emoji        = "🔴" if is_short else "🟢"
    armed_level  = LEGACY_RSI_OVERBOUGHT if is_short else LEGACY_RSI_OVERSOLD
    fire_level   = LEGACY_RSI_SHORT_CONFIRM if is_short else LEGACY_RSI_LONG_CONFIRM
    desc         = (f"đã vượt LÊN trên {armed_level} rồi lùi về tới {fire_level}" if is_short
                    else f"đã vượt XUỐNG dưới {armed_level} rồi tăng lên tới {fire_level}")
    return (
        f"*{emoji} {signal.direction} SIGNAL - {interval_display}*\n\n"
        f"Coin: `{signal.symbol}`\n\n"
        f"Điều kiện:\n"
        f"✓ RSI({LEGACY_RSI_PERIOD}) {desc} (trong cây H4 đang chạy)\n"
        f"✓ Tín hiệu INTRABAR — báo ngay lúc RSI chạm mốc {fire_level}, không chờ đóng nến\n\n"
        f"Entry: `{_fmt(signal.price)}`\n"
        f"TP: `{_fmt(tp)}` (chốt lời {LEGACY_TP_PCT*100:.1f}%)\n"
        f"SL: `{_fmt(signal.sl)}` (cắt lỗ {LEGACY_SL_PCT*100:.1f}%)"
    )


def _build_close_message(pos: Position, interval_display: str, hit: Literal["TP", "SL"]) -> str:
    level  = pos.tp if hit == "TP" else pos.sl
    emoji  = "✅" if hit == "TP" else "🛑"
    pct    = abs(level - pos.entry) / pos.entry * 100
    label  = "Chốt lời (TP)" if hit == "TP" else "Cắt lỗ (SL)"
    return (
        f"*{emoji} {label} — {pos.direction} {pos.symbol} - {interval_display}*\n\n"
        f"Entry: `{_fmt(pos.entry)}`\n"
        f"{hit}: `{_fmt(level)}` (~{pct:.1f}%)"
    )


def _build_channel_signal_message(pos: ChannelPosition) -> str:
    is_short = pos.direction == "SHORT"
    emoji    = "🔴" if is_short else "🟢"
    band     = "trên" if is_short else "dưới"
    return (
        f"*{emoji} {pos.direction} SIGNAL - {KEO_CHANNEL_NAME}*\n\n"
        f"Coin: `{pos.symbol}`\n\n"
        f"Điều kiện:\n"
        f"✓ Kênh song song dựng từ 3 điểm xoay gần nhất\n"
        f"✓ Giá quay lại CHẠM biên {band} (Line B) lần thứ 2 → đảo chiều\n\n"
        f"Entry: `{_fmt(pos.entry)}`\n"
        f"SL: `{_fmt(pos.sl)}` (cắt lỗ {CHANNEL_SL_PCT*100:.0f}%)\n"
        f"TP1: `{_fmt(pos.tp1)}` (chốt {CHANNEL_TP1_CLOSE_RATIO*100:.0f}%, {CHANNEL_TP1_PCT*100:.0f}%)\n"
        f"TP2: `{_fmt(pos.tp2)}` (chốt nốt, {CHANNEL_TP2_PCT*100:.0f}%)"
    )


def _build_channel_tp1_message(pos: ChannelPosition) -> str:
    pct = CHANNEL_TP1_PCT * 100
    return (
        f"*💰 TP1 — {pos.direction} {pos.symbol} - {KEO_CHANNEL_NAME}*\n\n"
        f"Entry: `{_fmt(pos.entry)}`\n"
        f"TP1: `{_fmt(pos.tp1)}` (~{pct:.0f}%) — chốt {CHANNEL_TP1_CLOSE_RATIO*100:.0f}%\n"
        f"SL phần còn lại: dời về entry `{_fmt(pos.entry)}` (breakeven)\n"
        f"Mục tiêu tiếp theo — TP2: `{_fmt(pos.tp2)}`"
    )


def _build_channel_close_message(pos: ChannelPosition, hit: Literal["TP2", "SL"]) -> str:
    if hit == "TP2":
        icon, label, level = "✅", "Chốt nốt (TP2)", pos.tp2
    elif pos.tp1_hit:
        icon, label, level = "🟡", "Về Entry (đã chốt TP1 trước đó)", pos.sl
    else:
        icon, label, level = "🛑", "Cắt lỗ (SL)", pos.sl
    pct = abs(level - pos.entry) / pos.entry * 100
    return (
        f"*{icon} {label} — {pos.direction} {pos.symbol} - {KEO_CHANNEL_NAME}*\n\n"
        f"Entry: `{_fmt(pos.entry)}`\n"
        f"{hit}: `{_fmt(level)}` (~{pct:.1f}%)"
    )


TELEGRAM_SEND_RETRIES     = 3     # Tổng số lần thử gửi 1 tin (1 lần đầu + 2 lần retry)
TELEGRAM_SEND_RETRY_DELAY = 2.0   # Giây chờ giữa mỗi lần retry

# Cửa sổ chống gửi TRÙNG: nếu ĐÚNG y hệt (chat_id, text) vừa được gửi cách đây chưa tới
# TELEGRAM_DEDUPE_WINDOW_SEC giây thì bỏ qua, không gửi lại — phòng trường hợp lần gửi
# TRƯỚC bị timeout ở PHÍA CLIENT (không nhận được response kịp trong 10s) nhưng thực ra
# Telegram ĐÃ nhận và gửi thành công, khiến lần retry sau đó gửi lại y nguyên -> trùng tin
# (đã xảy ra thực tế: 2 tin SHORT SIGNAL giống hệt entry/TP/SL gửi cách nhau vài giây).
TELEGRAM_DEDUPE_WINDOW_SEC = 180
_recent_sends: dict[tuple[str, str], datetime] = {}   # (chat_id, text) -> lúc gửi gần nhất


async def _send_telegram_message(chat_id: str, text: str, tag: str) -> None:
    """Gửi 1 tin Telegram, TỰ RETRY vài lần nếu lỗi mạng/API tạm thời — trước đây gửi lỗi là
    mất tin VĨNH VIỄN (chỉ log, không ai biết), từng gây hiện tượng "có tin đóng lệnh (TP/SL)
    nhưng KHÔNG có tin mở lệnh" dù lệnh thật vẫn mở/đóng đúng trên sàn (vd BSBUSDT: tin "Đã mở
    LONG" gửi thất bại 1 lần thoáng qua, còn tin "Đóng ... TP" sau đó gửi lại thành công bình
    thường nên vẫn thấy). KHÔNG retry nếu lỗi rõ ràng do payload sai (4xx do Markdown lỗi cú
    pháp...) vì gửi lại y nguyên cũng sẽ lỗi y như vậy — chỉ retry lỗi mạng/timeout/5xx/429.

    Trước khi gửi, kiểm tra dedupe (xem TELEGRAM_DEDUPE_WINDOW_SEC) — chặn gửi trùng y hệt nội
    dung cho cùng 1 chat trong cửa sổ ngắn, kể cả khi gọi hàm này từ 2 nơi độc lập."""
    key = (chat_id, text)
    now = datetime.now()
    # Dọn các mục đã hết hạn trước — tránh _recent_sends phình to vô hạn khi bot chạy 24/7
    # (mỗi entry chỉ sống tối đa TELEGRAM_DEDUPE_WINDOW_SEC).
    for k in [k for k, t in _recent_sends.items() if (now - t).total_seconds() >= TELEGRAM_DEDUPE_WINDOW_SEC]:
        del _recent_sends[k]

    last_sent = _recent_sends.get(key)
    if last_sent is not None and (now - last_sent).total_seconds() < TELEGRAM_DEDUPE_WINDOW_SEC:
        logger.warning(f"[TG-{tag}] Bỏ qua gửi TRÙNG — y hệt nội dung đã gửi cho chat này "
                        f"{int((now - last_sent).total_seconds())}s trước (< {TELEGRAM_DEDUPE_WINDOW_SEC}s)")
        return
    _recent_sends[key] = now

    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    last_error = ""
    for attempt in range(1, TELEGRAM_SEND_RETRIES + 1):
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        logger.info(f"[TG-{tag}] Gửi thành công" + (f" (lần {attempt})" if attempt > 1 else ""))
                        return
                    body = await resp.text()
                    last_error = f"HTTP {resp.status}: {body}"
                    if 400 <= resp.status < 500 and resp.status != 429:
                        # Lỗi phía payload (vd Markdown sai cú pháp, chat_id không hợp lệ) —
                        # gửi lại y nguyên chắc chắn lỗi lại, dừng ngay, khỏi retry vô ích.
                        logger.error(f"[TG-{tag}] Lỗi {last_error} — không retry (lỗi payload)")
                        return
                    logger.warning(f"[TG-{tag}] Lỗi {last_error} (lần {attempt}/{TELEGRAM_SEND_RETRIES})")
        except Exception as e:
            last_error = str(e)
            logger.warning(f"[TG-{tag}] Không gửi được: {e} (lần {attempt}/{TELEGRAM_SEND_RETRIES})")

        if attempt < TELEGRAM_SEND_RETRIES:
            await asyncio.sleep(TELEGRAM_SEND_RETRY_DELAY)

    logger.error(f"[TG-{tag}] Gửi THẤT BẠI sau {TELEGRAM_SEND_RETRIES} lần: {last_error}")


async def send_signal(signal: Signal, chat_id: str, interval_display: str, tp: float,
                       builder: Callable[[Signal, str, float], str] = _build_message) -> None:
    if not TELEGRAM_TOKEN or not chat_id:
        logger.warning(f"[{interval_display}] Chưa cấu hình TELEGRAM_TOKEN / chat ID")
        return
    text = builder(signal, interval_display, tp)
    await _send_telegram_message(chat_id, text, f"{interval_display}-{signal.direction}")


async def send_close_alert(pos: Position, chat_id: str, interval_display: str, hit: Literal["TP", "SL"]) -> None:
    if not TELEGRAM_TOKEN or not chat_id:
        logger.warning(f"[{interval_display}] Chưa cấu hình TELEGRAM_TOKEN / chat ID")
        return
    text = _build_close_message(pos, interval_display, hit)
    await _send_telegram_message(chat_id, text, f"{interval_display}-{hit}")


async def send_channel_signal(pos: ChannelPosition, chat_id: str) -> None:
    if not TELEGRAM_TOKEN or not chat_id:
        logger.warning(f"[{KEO_CHANNEL_NAME}] Chưa cấu hình TELEGRAM_TOKEN / chat ID")
        return
    await _send_telegram_message(chat_id, _build_channel_signal_message(pos), f"CHANNEL-{pos.direction}")


async def send_channel_tp1(pos: ChannelPosition, chat_id: str) -> None:
    if not TELEGRAM_TOKEN or not chat_id:
        return
    await _send_telegram_message(chat_id, _build_channel_tp1_message(pos), "CHANNEL-TP1")


async def send_channel_close(pos: ChannelPosition, chat_id: str, hit: Literal["TP2", "SL"]) -> None:
    if not TELEGRAM_TOKEN or not chat_id:
        return
    await _send_telegram_message(chat_id, _build_channel_close_message(pos, hit), f"CHANNEL-{hit}")

# ═══════════════════════════════════════════════
#  LIVE FEED (WEBSOCKET)
# ═══════════════════════════════════════════════
_FUTURES_REST = "https://fapi.binance.com"
_FUTURES_WS   = "wss://fstream.binance.com"
# Binance đã đổi kiến trúc WebSocket Futures: các stream được phân theo path /public, /market,
# /private (xem "Important WebSocket Change Notice"). Kline/kline_* thuộc nhóm /market — kết nối
# theo URL cũ (không có path) sau mốc chuyển đổi chỉ còn nhận được dữ liệu /public (vd: depth),
# KHÔNG còn nhận kline nữa (im lặng, không báo lỗi) dù handshake vẫn thành công bình thường.
# Vì vậy bắt buộc phải gọi qua wss://fstream.binance.com/market/stream?streams=...


async def fetch_top_symbols(n: int = 50) -> list[str]:
    url = f"{_FUTURES_REST}/fapi/v1/ticker/24hr"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
        pairs = [
            t for t in data
            if t["symbol"].endswith("USDT")
            and not any(x in t["symbol"] for x in ("UP", "DOWN", "BULL", "BEAR"))
        ]
        ranked  = sorted(pairs, key=lambda x: float(x["quoteVolume"]), reverse=True)
        symbols = [t["symbol"] for t in ranked[:n]]
        logger.info(f"Lấy được top {len(symbols)} cặp theo khối lượng")
        return symbols
    except Exception as e:
        logger.error(f"Không lấy được top symbol: {e}")
        return []


class LiveFeed:
    """Nhận dữ liệu nến real-time qua WebSocket kline stream của Binance Futures (path /market
    — xem ghi chú ở _FUTURES_WS) — thay cho REST polling định kỳ. Dùng CHUNG 1 nguồn cho tất
    cả scanner cùng interval, tránh mỗi scanner tự gọi REST lặp lại. WebSocket không có dữ
    liệu quá khứ nên vẫn cần REST để nạp lịch sử ban đầu, và để đồng bộ lại nếu kết nối bị rớt."""

    def __init__(self, symbols: list[str], interval: str, buffer_size: int) -> None:
        self.symbols     = sorted({s.upper() for s in symbols})
        self.interval    = interval
        self.buffer_size = buffer_size
        self.candles: dict[str, deque] = defaultdict(lambda: deque(maxlen=buffer_size))
        self._last_close: dict[str, int] = {}   # symbol -> close_time_ms đã xử lý
        self._closed_handlers: list[Callable[[str, list[dict]], Awaitable[None]]] = []
        self._live_handlers: list[Callable[[str, list[dict], dict], Awaitable[None]]] = []

    def on_closed_candle(self, handler: Callable[[str, list[dict]], Awaitable[None]]) -> None:
        """Đăng ký callback gọi khi 1 nến ĐÃ ĐÓNG (nhận (symbol, candles))."""
        self._closed_handlers.append(handler)

    def on_live_tick(self, handler: Callable[[str, list[dict], dict], Awaitable[None]]) -> None:
        """Đăng ký callback gọi mỗi khi có update giá cho nến ĐANG hình thành
        (nhận (symbol, closed_candles, live_candle))."""
        self._live_handlers.append(handler)

    async def _fetch_history(self, symbols: list[str]) -> None:
        """Nạp lịch sử nến qua REST — dùng lúc khởi động và khi cần đồng bộ lại sau khi mất kết nối."""
        logger.info(f"[LiveFeed-{self.interval}] Nạp lịch sử {len(symbols)} coin...")
        ok, fail = 0, []
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            for sym in symbols:
                for attempt in range(3):
                    try:
                        params = {"symbol": sym, "interval": self.interval, "limit": self.buffer_size}
                        async with session.get(
                            f"{_FUTURES_REST}/fapi/v1/klines", params=params,
                            timeout=aiohttp.ClientTimeout(total=15),
                        ) as resp:
                            if resp.status != 200:
                                raise ValueError(f"HTTP {resp.status}")
                            rows = await resp.json()
                            self.candles[sym].clear()
                            for k in rows[:-1]:   # bỏ nến đang mở
                                self.candles[sym].append({
                                    "open": float(k[1]), "high": float(k[2]),
                                    "low":  float(k[3]), "close": float(k[4]),
                                    "volume": float(k[5]),
                                    "bar_open": int(k[0]),   # k[0] = open time (ms) — đồng bộ với
                                                              # nến nạp qua WS (_handle_kline_event),
                                                              # cần cho kèo Kênh Song Song (điểm xoay
                                                              # dùng bar_open làm trục thời gian).
                                })
                            if rows:
                                self._last_close[sym] = int(rows[-2][6])
                        ok += 1
                        break
                    except Exception as e:
                        if attempt == 2:
                            logger.error(f"  ✗ {sym}: {e}")
                            fail.append(sym)
                        else:
                            await asyncio.sleep(1)
        logger.info(f"[LiveFeed-{self.interval}] Nạp xong {ok}/{len(symbols)}" +
                    (f" | Lỗi: {', '.join(fail)}" if fail else ""))

    async def _dispatch_closed(self, symbol: str, candles: list[dict]) -> None:
        for handler in self._closed_handlers:
            try:
                await handler(symbol, candles)
            except Exception as e:
                logger.error(f"[LiveFeed] Handler (closed) lỗi cho {symbol}: {e}", exc_info=True)

    async def _dispatch_live(self, symbol: str, candles: list[dict], live_candle: dict) -> None:
        for handler in self._live_handlers:
            try:
                await handler(symbol, candles, live_candle)
            except Exception as e:
                logger.error(f"[LiveFeed] Handler (live) lỗi cho {symbol}: {e}", exc_info=True)

    async def _handle_kline_event(self, symbol: str, k: dict) -> None:
        candle = {
            "open": float(k["o"]), "high": float(k["h"]),
            "low":  float(k["l"]), "close": float(k["c"]),
            "volume": float(k["v"]),
            "bar_open": int(k["t"]),   # mốc mở nến (ms, do Binance cấp) — dùng để nhận diện "cùng 1 nến"
        }
        if len(self.candles[symbol]) < MIN_CANDLES_FOR_SIGNAL:
            return

        if k["x"]:   # nến đã đóng
            close_time = int(k["T"])
            if self._last_close.get(symbol) == close_time:
                return
            self._last_close[symbol] = close_time
            self.candles[symbol].append(candle)
            await self._dispatch_closed(symbol, list(self.candles[symbol]))
        else:        # nến đang hình thành — báo real-time, không chờ đóng
            await self._dispatch_live(symbol, list(self.candles[symbol]), candle)

    async def _run_connection(self, chunk: list[str]) -> None:
        streams = "/".join(f"{s.lower()}@kline_{self.interval}" for s in chunk)
        url = f"{_FUTURES_WS}/market/stream?streams={streams}"
        while True:
            first_message  = True
            last_heartbeat = datetime.now()
            msg_count      = 0
            try:
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.ws_connect(url, heartbeat=180) as ws:
                        logger.info(f"[LiveFeed-{self.interval}] WS bắt tay thành công ({len(chunk)} coin), "
                                    f"đang chờ dữ liệu đầu tiên (timeout {WS_NO_DATA_TIMEOUT_SEC}s)...")
                        while True:
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=WS_NO_DATA_TIMEOUT_SEC)
                            except asyncio.TimeoutError:
                                print(f"⚠️ [LiveFeed-{self.interval}] KHÔNG nhận được bất kỳ dữ liệu nào trong "
                                      f"{WS_NO_DATA_TIMEOUT_SEC}s ({len(chunk)} coin) — kết nối lại...")
                                logger.warning(f"[LiveFeed-{self.interval}] Timeout không có dữ liệu "
                                               f"({WS_NO_DATA_TIMEOUT_SEC}s, {len(chunk)} coin), kết nối lại")
                                raise ConnectionError("Không nhận được dữ liệu — timeout watchdog")

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                payload = json.loads(msg.data)
                                data = payload.get("data", payload)
                                k = data.get("k")
                                if k is None:
                                    logger.info(f"[LiveFeed-{self.interval}] Nhận message không phải kline: "
                                                f"{str(msg.data)[:200]}")
                                    continue
                                msg_count += 1
                                if first_message:
                                    first_message = False
                                    print(f"✅ [LiveFeed-{self.interval}] Đã NHẬN được dữ liệu real-time từ Binance "
                                          f"({len(chunk)} coin) — mẫu: {data['s']} close={k['c']} "
                                          f"đóng={k['x']}")
                                    logger.info(f"[LiveFeed-{self.interval}] Xác nhận nhận dữ liệu real-time OK "
                                                f"({len(chunk)} coin) — mẫu: {data['s']} close={k['c']}")

                                now = datetime.now()
                                if now - last_heartbeat >= timedelta(minutes=WS_HEARTBEAT_MINUTES):
                                    last_heartbeat = now
                                    print(f"✅ [LiveFeed-{self.interval}] Vẫn đang kết nối Binance OK "
                                          f"({len(chunk)} coin, đã nhận {msg_count} update) — "
                                          f"mẫu: {data['s']} close={k['c']}")
                                    logger.info(f"[LiveFeed-{self.interval}] Heartbeat — vẫn kết nối OK "
                                                f"({len(chunk)} coin, {msg_count} update đã nhận)")

                                await self._handle_kline_event(data["s"], k)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE,
                                               aiohttp.WSMsgType.ERROR):
                                raise ConnectionError(f"WS đóng/lỗi: {msg}")
                            else:
                                logger.info(f"[LiveFeed-{self.interval}] Nhận message loại khác: "
                                            f"{msg.type} — data={str(msg.data)[:200]}")
            except Exception as e:
                logger.error(f"[LiveFeed-{self.interval}] WS lỗi ({len(chunk)} coin), "
                             f"đồng bộ lại + kết nối lại sau {WS_RECONNECT_DELAY_SEC}s: {e}")
                try:
                    await self._fetch_history(chunk)
                except Exception as e2:
                    logger.error(f"[LiveFeed-{self.interval}] Đồng bộ lại thất bại: {e2}")
                await asyncio.sleep(WS_RECONNECT_DELAY_SEC)

    async def run(self) -> None:
        await self._fetch_history(self.symbols)
        chunks = [self.symbols[i:i + WS_MAX_STREAMS_PER_CONN]
                  for i in range(0, len(self.symbols), WS_MAX_STREAMS_PER_CONN)]
        logger.info(f"[LiveFeed-{self.interval}] Mở {len(chunks)} kết nối WS cho {len(self.symbols)} coin")
        await asyncio.gather(*(self._run_connection(c) for c in chunks))

# ═══════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════
async def resolve_symbols(count: int = TOP_SYMBOLS_COUNT) -> list[str]:
    if AUTO_TOP_SYMBOLS:
        logger.info(f"Lấy top {count} cặp từ Binance...")
        symbols = await fetch_top_symbols(count)
        if not symbols:
            logger.warning("Không lấy được, dùng danh sách cố định")
            symbols = list(SYMBOLS)
    else:
        symbols = list(SYMBOLS)
        logger.info(f"Dùng {len(symbols)} cặp từ cấu hình")
    return symbols


class Scanner:
    """Xử lý tín hiệu cho 1 bộ điều kiện (LONG-H1 / SHORT-H1 / LONG-H1 cũ / SHORT-H1 cũ). Không tự lấy
    dữ liệu — nhận candles từ LiveFeed dùng chung qua on_closed_candle/on_live_tick."""

    def __init__(self, symbols: list[str], interval_display: str,
                 chat_id: str, detect_fn: Callable[[str, list[dict]], Signal | None],
                 tp_pct: float, sl_pct: float,
                 message_builder: Callable[[Signal, str, float], str] = _build_message,
                 cooldown_minutes: int = ALERT_COOLDOWN_MINUTES,
                 daily_stats: "DailyStats | None" = None) -> None:
        self.symbols          = {s.upper() for s in symbols}
        self.interval_display = interval_display
        self.chat_id          = chat_id
        self.detect_fn        = detect_fn
        self.tp_pct           = tp_pct
        self.sl_pct           = sl_pct
        self.message_builder  = message_builder
        self.cooldown_minutes = cooldown_minutes
        self.daily_stats      = daily_stats   # DailyStats dùng CHUNG với scanner chiều đối diện cùng kèo
        self._last_alert: dict[str, datetime] = {}
        self._positions: dict[str, Position] = {}   # symbol -> lệnh đang mở

    def _cooldown_left(self, symbol: str, direction: str) -> int:
        last = self._last_alert.get(f"{symbol}_{direction}")
        if last is None:
            return 0
        remaining = timedelta(minutes=self.cooldown_minutes) - (datetime.now() - last)
        return max(0, int(remaining.total_seconds()))

    async def _check_position(self, symbol: str, candle: dict) -> None:
        """Kiểm tra lệnh đang mở của coin này đã chạm TP hay SL chưa (gọi được cả lúc
        nến đóng lẫn real-time theo từng tick giá)."""
        pos = self._positions.get(symbol)
        if pos is None:
            return
        hit = _position_hit(pos, candle)
        if hit is None:
            return

        logger.info(f"[{self.interval_display}] {symbol} {pos.direction} {hit} | "
                    f"Entry={pos.entry:.4f}  {hit}={(pos.tp if hit == 'TP' else pos.sl):.4f}")
        if self.daily_stats is not None:
            self.daily_stats.record_result(hit)
        await send_close_alert(pos, self.chat_id, self.interval_display, hit)
        del self._positions[symbol]

    async def on_closed_candle(self, symbol: str, candles: list[dict]) -> None:
        if symbol not in self.symbols:
            return

        await self._check_position(symbol, candles[-1])
        if symbol in self._positions:
            return   # Lệnh của coin này vẫn đang mở, chưa tìm tín hiệu mới

        signal = self.detect_fn(symbol, candles)
        if signal is None:
            return

        left = self._cooldown_left(symbol, signal.direction)
        if left > 0:
            m, s = divmod(left, 60)
            logger.info(f"[{self.interval_display}] {symbol} {signal.direction}: cooldown còn {m}p{s:02d}s")
            return

        self._last_alert[f"{symbol}_{signal.direction}"] = datetime.now()

        if signal.direction == "SHORT":
            tp = signal.price * (1 - self.tp_pct)
            signal.sl = signal.price * (1 + self.sl_pct)
        else:
            tp = signal.price * (1 + self.tp_pct)
            signal.sl = signal.price * (1 - self.sl_pct)
        self._positions[symbol] = Position(
            symbol=symbol, direction=signal.direction, entry=signal.price,
            tp=tp, sl=signal.sl, opened_at=datetime.now(),
        )

        logger.info(f">>> [{self.interval_display}] TÍN HIỆU: {symbol} {signal.direction} | "
                    f"Entry={signal.price} | TP={tp} | SL={signal.sl}")
        if self.daily_stats is not None:
            self.daily_stats.record_open()
        await send_signal(signal, self.chat_id, self.interval_display, tp, self.message_builder)

    async def on_live_tick(self, symbol: str, candles: list[dict], live_candle: dict) -> None:
        """Check TP/SL real-time theo từng tick giá, không chờ nến đóng."""
        if symbol not in self.symbols:
            return
        await self._check_position(symbol, live_candle)


# SpikeScanner (kèo BB H1 Đột Biến) và MidCrossScanner (kèo BB RSI H1) đã bị XÓA — 2 kèo đó
# KHÔNG còn chạy trong bot (cùng với executor.py, vốn chỉ 2 kèo này dùng để auto-trade thật).


class RsiExtremeScanner:
    """Kèo RSI H4 Đảo Biên — chạy trên feed H4 RIÊNG (khác feed H1 dùng chung của các kèo
    khác). Tín hiệu INTRABAR, xét trên MỌI tick giá (kể cả lúc nến H4 CHƯA đóng cửa). Mốc
    armed và mốc xác nhận bắn TÁCH RIÊNG (đệm 5 điểm RSI, lọc bớt tín hiệu nhiễu sát biên):
      SHORT: RSI(6) đã từng vượt LÊN trên LEGACY_RSI_OVERBOUGHT (90) trong cây H4 đang chạy
             (armed), rồi sau đó lùi về tới LEGACY_RSI_SHORT_CONFIRM (85) -> báo NGAY.
      LONG:  RSI(6) đã từng vượt XUỐNG dưới LEGACY_RSI_OVERSOLD (10) trong cây H4 đang chạy
             (armed), rồi sau đó tăng lên tới LEGACY_RSI_LONG_CONFIRM (15) -> báo NGAY.
    Trạng thái armed chỉ có hiệu lực TRONG PHẠM VI 1 cây H4 đang hình thành — tự reset về
    chưa-armed mỗi khi phát hiện bar_open đổi (nến H4 mới mở), không cộng dồn qua nhiều cây.
    TP/SL là mốc giá cố định (+LEGACY_TP_PCT/-LEGACY_SL_PCT), dùng chung Position +
    _position_hit() như các kèo khác. Chỉ báo Telegram — KHÔNG tự đặt lệnh thật (kèo này
    không nhận executor)."""

    def __init__(self, symbols: list[str], chat_id: str,
                 daily_stats: "DailyStats | None" = None) -> None:
        self.symbols     = {s.upper() for s in symbols}
        self.chat_id     = chat_id
        self.daily_stats = daily_stats
        self._last_alert: dict[str, datetime] = {}
        self._positions: dict[str, Position] = {}
        # Trạng thái "armed" theo dõi TRONG cây H4 đang chạy — self._bar_open ghi nhớ bar_open
        # đang được xét để biết khi nào cây H4 MỚI mở (khác bar_open) thì phải reset 2 cờ dưới.
        self._bar_open:    dict[str, int]  = {}
        self._short_armed: dict[str, bool] = {}   # RSI đã vượt lên >90 trong cây này chưa
        self._long_armed:  dict[str, bool] = {}   # RSI đã vượt xuống <10 trong cây này chưa

    def _cooldown_left(self, symbol: str) -> int:
        """Cooldown tính TỪ LÚC VÀO LỆNH (không phải từ lúc đóng lệnh) — 1 cặp vào lệnh xong bị
        khoá cả 2 chiều LONG/SHORT trong LEGACY_ALERT_COOLDOWN_MINUTES, kể cả khi lệnh đó đã
        đóng (TP/SL) sớm hơn khoảng thời gian này."""
        last = self._last_alert.get(symbol)
        if last is None:
            return 0
        remaining = timedelta(minutes=LEGACY_ALERT_COOLDOWN_MINUTES) - (datetime.now() - last)
        return max(0, int(remaining.total_seconds()))

    async def _check_position(self, symbol: str, candle: dict) -> None:
        pos = self._positions.get(symbol)
        if pos is None:
            return
        hit = _position_hit(pos, candle)
        if hit is None:
            return

        logger.info(f"[H4-RSI] {symbol} {pos.direction} {hit} | "
                    f"Entry={pos.entry:.4f}  {hit}={(pos.tp if hit == 'TP' else pos.sl):.4f}")
        if self.daily_stats is not None:
            self.daily_stats.record_result(hit)
        await send_close_alert(pos, self.chat_id, KEO_LEGACY_NAME, hit)
        del self._positions[symbol]

    def _reset_arm_if_new_bar(self, symbol: str, bar_open: int | None) -> None:
        """Nến H4 mới mở (bar_open đổi) -> trạng thái armed của cây CŨ hết hiệu lực, xét lại
        từ đầu cho cây mới, đúng nghĩa "theo dõi RSI liên tục TRONG cây H4 đang chạy"."""
        if bar_open is None or self._bar_open.get(symbol) == bar_open:
            return
        self._bar_open[symbol]    = bar_open
        self._short_armed[symbol] = False
        self._long_armed[symbol]  = False

    async def _fire(self, symbol: str, direction: Literal["LONG", "SHORT"],
                     price: float, rsi: float, bar_open: int | None) -> None:
        if symbol in self._positions:
            return   # Lệnh của coin này vẫn đang mở, chưa mở lệnh mới
        left = self._cooldown_left(symbol)
        if left > 0:
            m, s = divmod(left, 60)
            logger.info(f"[H4-RSI] {symbol} {direction}: cooldown còn {m}p{s:02d}s")
            return
        self._last_alert[symbol] = datetime.now()

        empty_ind: Indicators = {"bb_upper": 0.0, "bb_middle": 0.0, "bb_lower": 0.0}   # kèo này không dùng BB
        signal = Signal(symbol=symbol, direction=direction, price=price, sl=0.0, ind=empty_ind)
        if direction == "SHORT":
            tp = price * (1 - LEGACY_TP_PCT)
            signal.sl = price * (1 + LEGACY_SL_PCT)
        else:
            tp = price * (1 + LEGACY_TP_PCT)
            signal.sl = price * (1 - LEGACY_SL_PCT)

        self._positions[symbol] = Position(
            symbol=symbol, direction=direction, entry=price,
            tp=tp, sl=signal.sl, opened_at=datetime.now(),
            entry_bar_open=bar_open,   # xem _position_hit(): tránh tính lùi high/low đã có
                                         # TRƯỚC lúc vào lệnh trong CHÍNH cây H4 vừa xuyên biên
        )
        logger.info(f">>> [H4-RSI] TÍN HIỆU: {symbol} {direction} | RSI={rsi:.1f} | "
                    f"Entry={price} | TP={tp} | SL={signal.sl}")
        if self.daily_stats is not None:
            self.daily_stats.record_open()
        await send_signal(signal, self.chat_id, KEO_LEGACY_NAME, tp, _build_h4_rsi_message)

    async def _check_signal(self, symbol: str, closed_candles: list[dict], live_candle: dict) -> None:
        if len(closed_candles) < LEGACY_RSI_PERIOD + 1:
            return
        self._reset_arm_if_new_bar(symbol, live_candle.get("bar_open"))

        closes   = [c["close"] for c in closed_candles] + [live_candle["close"]]
        live_rsi = _calc_rsi(closes, LEGACY_RSI_PERIOD)[-1]
        bar_open = live_candle.get("bar_open")

        if self._short_armed.get(symbol) and live_rsi <= LEGACY_RSI_SHORT_CONFIRM:
            self._short_armed[symbol] = False   # tiêu thụ trạng thái armed ngay, tránh báo lặp
            await self._fire(symbol, "SHORT", live_candle["close"], live_rsi, bar_open)
        elif live_rsi > LEGACY_RSI_OVERBOUGHT:
            self._short_armed[symbol] = True

        if self._long_armed.get(symbol) and live_rsi >= LEGACY_RSI_LONG_CONFIRM:
            self._long_armed[symbol] = False
            await self._fire(symbol, "LONG", live_candle["close"], live_rsi, bar_open)
        elif live_rsi < LEGACY_RSI_OVERSOLD:
            self._long_armed[symbol] = True

    async def on_closed_candle(self, symbol: str, candles: list[dict]) -> None:
        if symbol not in self.symbols:
            return
        await self._check_position(symbol, candles[-1])
        if len(candles) >= 2:
            # Nến vừa đóng cũng là 1 "tick" hợp lệ để xét tín hiệu — dùng candles[:-1] làm
            # lịch sử đã đóng, candles[-1] (vừa đóng) làm mốc "live" cuối cùng của cây đó,
            # đảm bảo không bỏ sót đúng lúc chuyển sang cây H4 mới.
            await self._check_signal(symbol, candles[:-1], candles[-1])

    async def on_live_tick(self, symbol: str, candles: list[dict], live_candle: dict) -> None:
        if symbol not in self.symbols:
            return
        await self._check_position(symbol, live_candle)
        await self._check_signal(symbol, candles, live_candle)


class ChannelScanner:
    """Kèo Kênh Song Song 3 Điểm — xem khai báo CHANNEL_* để rõ tham số. Chạy trên feed H1
    dùng CHUNG với kèo Rút Râu (không cần LiveFeed riêng). Chỉ báo Telegram — KHÔNG tự đặt
    lệnh thật (không nhận executor, giống 2 kèo còn lại).

    Luồng xử lý mỗi khi có nến H1 mới đóng, cho từng symbol CHƯA có lệnh đang theo dõi:
    1. Tính lại kênh (Line A/Line B) từ 3 điểm xoay XÁC NHẬN gần nhất (_find_swing_points +
       _build_channel). Nếu điểm xoay mới nhất (p3) khác lần tính trước -> kênh vừa đổi, THAY
       kênh cũ (coi như giá chưa chạm Line B lần nào với kênh mới này) — đúng thiết kế "kênh
       cập nhật liên tục khi CHƯA vào lệnh".
    2. Nếu nến hiện tại CHẠM Line B của kênh đang lưu -> vào lệnh ĐẢO CHIỀU (SHORT nếu Line B
       là biên trên, LONG nếu là biên dưới), SL/TP1/TP2 = % CỐ ĐỊNH từ entry (không bám theo
       Line A/Line B nữa) — 3 kèo còn lại cũng dùng % cố định, giữ đồng bộ.

    Sau khi vào lệnh, kênh KHÔNG cập nhật nữa (đóng băng) cho tới khi lệnh đóng hẳn (TP2 hoặc
    SL) — tránh SL/TP nhảy theo dữ liệu mới trong lúc lệnh đang chạy, giống các kèo khác.
    Quản lý 2 chặng: chạm TP1 -> chốt CHANNEL_TP1_CLOSE_RATIO khối lượng + dời SL về ĐÚNG giá
    entry (breakeven, hết rủi ro), KHÔNG xoá theo dõi — vẫn chờ tiếp TP2 hoặc SL (breakeven).
    Chạm TP2 -> chốt nốt, xoá theo dõi. Chạm SL trước khi kịp TP1 -> cắt lỗ thật, xoá theo dõi."""

    def __init__(self, symbols: list[str], chat_id: str,
                 daily_stats: "DailyStats | None" = None) -> None:
        self.symbols     = {s.upper() for s in symbols}
        self.chat_id     = chat_id
        self.daily_stats = daily_stats
        self._channel:   dict[str, Channel] = {}            # symbol -> kênh hiện tại
        self._positions: dict[str, ChannelPosition] = {}
        self._last_alert: dict[str, datetime] = {}

    def _cooldown_left(self, symbol: str) -> int:
        last = self._last_alert.get(symbol)
        if last is None:
            return 0
        remaining = timedelta(minutes=ALERT_COOLDOWN_MINUTES) - (datetime.now() - last)
        return max(0, int(remaining.total_seconds()))

    def _refresh_channel(self, symbol: str, candles: list[dict]) -> None:
        """Tính lại kênh từ 3 điểm xoay MỚI NHẤT. Chỉ THAY kênh đang lưu nếu điểm xoay mới
        nhất (p3) thật sự khác — tránh dựng lại (và "quên" đã chạm Line B lần nào) mỗi lần gọi
        dù chưa có điểm xoay mới nào xác nhận thêm."""
        points = _find_swing_points(candles, CHANNEL_FRACTAL_K, count=3)
        if points is None:
            return
        channel = _build_channel(points)
        if channel is None:
            return
        old = self._channel.get(symbol)
        if old is None or old["p3_bar_open"] != channel["p3_bar_open"]:
            self._channel[symbol] = channel

    def _touches_line_b(self, channel: Channel, candle: dict) -> bool:
        bar_open = candle.get("bar_open")
        if bar_open is None:
            return False
        line_b = channel["lineB_at"](bar_open)
        if channel["direction"] == "SHORT":
            return candle["high"] >= line_b
        return candle["low"] <= line_b

    async def _enter(self, symbol: str, channel: Channel, candle: dict) -> None:
        left = self._cooldown_left(symbol)
        if left > 0:
            return
        self._last_alert[symbol] = datetime.now()

        direction = channel["direction"]
        entry = candle["close"]
        if direction == "SHORT":
            sl  = entry * (1 + CHANNEL_SL_PCT)
            tp1 = entry * (1 - CHANNEL_TP1_PCT)
            tp2 = entry * (1 - CHANNEL_TP2_PCT)
        else:
            sl  = entry * (1 - CHANNEL_SL_PCT)
            tp1 = entry * (1 + CHANNEL_TP1_PCT)
            tp2 = entry * (1 + CHANNEL_TP2_PCT)

        pos = ChannelPosition(symbol=symbol, direction=direction, entry=entry,
                               sl=sl, tp1=tp1, tp2=tp2, opened_at=datetime.now())
        self._positions[symbol] = pos
        if self.daily_stats is not None:
            self.daily_stats.record_open()

        logger.info(f">>> [CHANNEL] TÍN HIỆU: {symbol} {direction} | Entry={entry} | "
                    f"SL={sl} TP1={tp1} TP2={tp2}")
        await send_channel_signal(pos, self.chat_id)

    async def _check_position(self, symbol: str, candle: dict) -> None:
        pos = self._positions.get(symbol)
        if pos is None:
            return
        hit = _channel_position_hit(pos, candle)
        if hit is None:
            return

        if hit == "TP1":
            pos.tp1_hit = True
            pos.sl = pos.entry   # dời SL về breakeven — hết rủi ro cho phần còn lại
            logger.info(f"[CHANNEL] {symbol} {pos.direction} TP1 (+{CHANNEL_TP1_PCT*100:.0f}%) -> "
                        f"chốt {CHANNEL_TP1_CLOSE_RATIO*100:.0f}%, dời SL về entry")
            await send_channel_tp1(pos, self.chat_id)
            return   # KHÔNG xoá theo dõi — vẫn chờ TP2 hoặc SL (breakeven) cho phần còn lại

        logger.info(f"[CHANNEL] {symbol} {pos.direction} {hit} | Entry={pos.entry:.4f}")
        if self.daily_stats is not None:
            # Đã chốt TP1 trước đó (dù sau đó về breakeven) vẫn tính THẮNG — thực tế đã có lời.
            self.daily_stats.record_result("TP" if (hit == "TP2" or pos.tp1_hit) else "SL")
        await send_channel_close(pos, self.chat_id, hit)
        del self._positions[symbol]

    async def on_closed_candle(self, symbol: str, candles: list[dict]) -> None:
        if symbol not in self.symbols:
            return
        await self._check_position(symbol, candles[-1])
        if symbol in self._positions:
            return   # Lệnh đang mở/theo dõi -> đóng băng kênh, không tính lại (xem docstring)

        self._refresh_channel(symbol, candles)
        channel = self._channel.get(symbol)
        if channel is not None and self._touches_line_b(channel, candles[-1]):
            await self._enter(symbol, channel, candles[-1])

    async def on_live_tick(self, symbol: str, candles: list[dict], live_candle: dict) -> None:
        if symbol not in self.symbols:
            return
        await self._check_position(symbol, live_candle)
        if symbol in self._positions:
            return

        channel = self._channel.get(symbol)
        if channel is not None and self._touches_line_b(channel, live_candle):
            await self._enter(symbol, channel, live_candle)

# ═══════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════
def _banner() -> None:
    logger.info("=" * 50)
    logger.info("  Binance Futures Song Kiem Signal Bot (WebSocket real-time)")
    logger.info("=" * 50)
    if AUTO_TOP_SYMBOLS:
        logger.info(f"  Symbol    : Tự động top {TOP_SYMBOLS_COUNT} (LONG/SHORT) | top {LEGACY_TOP_SYMBOLS_COUNT} (RSI H4)")
    else:
        logger.info(f"  Symbol    : Thủ công {len(SYMBOLS)} cặp")
    logger.info(f"  Timeframe : {INTERVAL_H1_DISPLAY} (kèo Rút Râu + Kênh Song Song) | "
                f"{INTERVAL_H4_DISPLAY} (kèo RSI Đảo Biên, LiveFeed riêng)")
    logger.info(f"  Nguồn nến : Futures WebSocket (path /market)")
    logger.info(f"  {KEO_RUTRAU_NAME:<22} -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID else 'OK'}  "
                f"TP/SL={DOJI_TP_PCT*100:.1f}%/{DOJI_SL_PCT*100:.1f}%")
    logger.info(f"  {KEO_LEGACY_NAME:<22} -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID_H1 else 'OK'}  "
                f"TP/SL={LEGACY_TP_PCT*100:.1f}%/{LEGACY_SL_PCT*100:.1f}%  "
                f"RSI({LEGACY_RSI_PERIOD}) armed={LEGACY_RSI_OVERSOLD}/{LEGACY_RSI_OVERBOUGHT} "
                f"bắn={LEGACY_RSI_LONG_CONFIRM}/{LEGACY_RSI_SHORT_CONFIRM} (intrabar)")
    logger.info(f"  {KEO_CHANNEL_NAME:<22} -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID_CHANNEL else 'OK'}  "
                f"SL/TP1/TP2={CHANNEL_SL_PCT*100:.0f}%/{CHANNEL_TP1_PCT*100:.0f}%/{CHANNEL_TP2_PCT*100:.0f}%  "
                f"fractal_k={CHANNEL_FRACTAL_K}")
    logger.info(f"  BB        : period={BB_PERIOD}  std={BB_STD}")
    logger.info(f"  Cooldown  : {ALERT_COOLDOWN_MINUTES} phút (mới/đột biến/kênh)  |  "
                f"{LEGACY_ALERT_COOLDOWN_MINUTES // 60} tiếng (RSI H4)")
    logger.info("=" * 50)


async def _check_telegram_connection(chat_id: str, label: str) -> None:
    """Chỉ kiểm tra token + chat_id có hợp lệ không (qua getChat) — KHÔNG gửi tin nhắn vào chat."""
    if not TELEGRAM_TOKEN or not chat_id:
        print("=" * 50)
        print(f"  [LỖI] Chưa điền TELEGRAM_TOKEN hoặc chat ID cho {label}")
        print("=" * 50)
        logger.warning(f"[TG-{label}] Chưa cấu hình TELEGRAM_TOKEN / chat ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getChat"
    print(f"  Đang kiểm tra kết nối Telegram ({label})...")
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, params={"chat_id": chat_id},
                                    timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    print(f"  [TELEGRAM] Kết nối thành công ✓ ({label})")
                    logger.info(f"[TG-{label}] Kết nối Telegram OK (chat_id={chat_id})")
                else:
                    body = await resp.text()
                    print(f"  [LỖI TELEGRAM] HTTP {resp.status} ({label}) — kiểm tra lại token/chat ID")
                    logger.error(f"[TG-{label}] Lỗi {resp.status}: {body}")
    except Exception as e:
        print(f"  [LỖI TELEGRAM] Không kết nối được ({label}): {e}")
        logger.error(f"[TG-{label}] Không kết nối được: {e}")


async def _check_telegram_connections() -> None:
    await _check_telegram_connection(TELEGRAM_CHAT_ID, "NEW-H1")
    await _check_telegram_connection(TELEGRAM_CHAT_ID_H1, "RSI-H4")
    await _check_telegram_connection(TELEGRAM_CHAT_ID_CHANNEL, "CHANNEL")


async def _run_forever(label: str, feed: LiveFeed) -> None:
    """Chạy LiveFeed vô hạn; nếu crash bất ngờ (hiếm, vì LiveFeed đã tự retry nội bộ)
    thì log + khởi động lại thay vì để cả bot dừng hẳn."""
    while True:
        try:
            await feed.run()
        except Exception as e:
            logger.critical(f"[{label}] LiveFeed dừng bất ngờ, khởi động lại sau 10s: {e}", exc_info=True)
            await asyncio.sleep(10)


async def daily_stats_scheduler(stats_list: list[DailyStats]) -> None:
    """Gửi thống kê cuối ngày cho từng kèo lúc 23:55 — mỗi kèo về đúng kênh của nó, ước lượng
    theo nến (DailyStats.build_message). Reset lại bộ đếm sau khi gửi.

    (Trước đây còn nhận thêm executor + executor_targets để gửi thống kê PnL THẬT cho 2 kèo
    auto-trade (Đột Biến, BB RSI H1) — đã bỏ cùng lúc xóa 2 kèo đó + executor.py.)"""
    while True:
        now = datetime.now()
        target = now.replace(hour=23, minute=55, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        wait = (target - now).total_seconds()
        logger.info(f"[DailyStats] Thống kê tiếp theo lúc {target.strftime('%H:%M %d/%m')} "
                    f"(còn {wait / 3600:.1f}h)")
        await asyncio.sleep(wait)

        today_str = datetime.now().strftime("%d/%m/%Y")
        for stats in stats_list:
            try:
                await _send_telegram_message(stats.chat_id, stats.build_message(today_str), f"STATS-{stats.name}")
            except Exception as e:
                logger.error(f"[DailyStats] Gửi thống kê {stats.name} lỗi: {e}")
            stats.reset()

        await asyncio.sleep(61)   # tránh kích hoạt 2 lần trong cùng phút


async def _main() -> None:
    _banner()
    await _check_telegram_connections()
    try:
        symbols        = await resolve_symbols(TOP_SYMBOLS_COUNT)
        legacy_symbols = await resolve_symbols(LEGACY_TOP_SYMBOLS_COUNT)

        # Kèo RSI H4 Đảo Biên chạy trên feed H4 RIÊNG (khác kèo Rút Râu dùng feed H1) vì
        # Binance kline stream chỉ phát 1 interval/stream — cần 1 LiveFeed độc lập cho H4.
        feed    = LiveFeed(symbols, INTERVAL_H1, CANDLE_BUFFER)
        feed_h4 = LiveFeed(legacy_symbols, INTERVAL_H4, CANDLE_BUFFER)

        h1_stats      = DailyStats(KEO_RUTRAU_NAME, TELEGRAM_CHAT_ID)
        legacy_stats  = DailyStats(KEO_LEGACY_NAME, TELEGRAM_CHAT_ID_H1)
        channel_stats = DailyStats(KEO_CHANNEL_NAME, TELEGRAM_CHAT_ID_CHANNEL)

        long_scanner = Scanner(
            symbols, KEO_RUTRAU_NAME, TELEGRAM_CHAT_ID,
            detect_fn=lambda s, c: detect_signal(s, c, direction="LONG"),
            tp_pct=DOJI_TP_PCT, sl_pct=DOJI_SL_PCT,
            daily_stats=h1_stats,
        )
        short_scanner = Scanner(
            symbols, KEO_RUTRAU_NAME, TELEGRAM_CHAT_ID,
            detect_fn=lambda s, c: detect_signal(s, c, direction="SHORT"),
            tp_pct=DOJI_TP_PCT, sl_pct=DOJI_SL_PCT,
            daily_stats=h1_stats,
        )
        channel_scanner = ChannelScanner(
            symbols, TELEGRAM_CHAT_ID_CHANNEL, daily_stats=channel_stats,
        )
        h4_rsi_scanner = RsiExtremeScanner(
            legacy_symbols, TELEGRAM_CHAT_ID_H1, daily_stats=legacy_stats,
        )

        for sc in (long_scanner, short_scanner, channel_scanner):
            feed.on_closed_candle(sc.on_closed_candle)
            feed.on_live_tick(sc.on_live_tick)

        feed_h4.on_closed_candle(h4_rsi_scanner.on_closed_candle)
        feed_h4.on_live_tick(h4_rsi_scanner.on_live_tick)

        stats_tasks = [daily_stats_scheduler([h1_stats, legacy_stats, channel_stats])]

        await asyncio.gather(
            _run_forever("LiveFeed-H1", feed),
            _run_forever("LiveFeed-H4", feed_h4),
            *stats_tasks,
        )
    except KeyboardInterrupt:
        logger.info("Bot dừng.")
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(_main())
