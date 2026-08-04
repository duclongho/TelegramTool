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

try:
    from executor import ENABLE_AUTO_TRADE, TradeExecutor
except ImportError:
    # Chưa cài python-binance / chưa có executor.py -> bot vẫn chạy bình thường,
    # chỉ không có phần tự động đặt lệnh (chỉ gửi Telegram như trước giờ).
    ENABLE_AUTO_TRADE = False
    TradeExecutor = None

# TODO(demo-test): TẠM BẬT — kênh Spike hiển thị NHƯ BÌNH THƯỜNG (báo tín hiệu + TP/SL
# theo nến, bất kể có executor hay không); executor vẫn chạy ngầm đặt lệnh demo thật
# nhưng CHỈ GHI LOG, không gửi Telegram. Sau khi test xong, đổi lại thành False để trả
# về hành vi: có executor thì CHỈ báo theo lệnh thật (xem chỗ dùng biến này trong
# SpikeScanner.on_live_tick/_check_position và _main()).
EXECUTOR_SILENT_DURING_TEST = True

# ═══════════════════════════════════════════════
#  CẤU HÌNH — chỉnh ở đây
# ═══════════════════════════════════════════════
TELEGRAM_TOKEN         = "8641278115:AAEB08VXrX5YJl_2zzM_SFF4JRdEwIfAj-s"   # Token bot Telegram
TELEGRAM_CHAT_ID       = "-1004448248877"   # Chat ID nhận LONG SIGNAL (H1) + SHORT SIGNAL (H1) — điều kiện mới, chạm BB
TELEGRAM_CHAT_ID_H1    = "-1004340326145"   # Chat ID nhận tín hiệu LONG/SHORT (H1) cũ — đóng cửa vượt BB dưới/trên, TP/SL cũ
TELEGRAM_CHAT_ID_SPIKE = "-1003980035281"   # : Chat ID nhận LONG/SHORT SIGNAL ĐỘT BIẾN (real-time)

AUTO_TOP_SYMBOLS  = True   # True = tự động lấy top coin theo khối lượng
TOP_SYMBOLS_COUNT = 200     # Số lượng coin theo dõi (LONG-H1 + SHORT-H1 mới)
LEGACY_TOP_SYMBOLS_COUNT = 150   # Số lượng coin theo dõi riêng cho LONG/SHORT-H1 cũ

SYMBOLS = [                # Dùng khi AUTO_TOP_SYMBOLS = False
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

CANDLE_BUFFER = 150

INTERVAL_H1         = "1h"   # Timeframe dùng chung cho tất cả các kèo
INTERVAL_H1_DISPLAY = "H1"

BB_PERIOD = 20
BB_STD    = 2.0

DOJI_BODY_MAX_RATIO       = 0.3   # Thân nến tối đa 30% tổng biên độ nến (high-low) — coi là nến doji
DOJI_SHORT_WICK_MAX_RATIO = 0.1   # Râu phía đối diện hướng đảo chiều tối đa 10% tổng biên độ (gần như không có)
BAND_CROSS_MIN_RATIO      = 0.1   # Phần xuyên qua BB trên/dưới tối thiểu 10% tổng biên độ nến

LEGACY_BAND_CROSS_MIN_RATIO = 0.9   # Kèo cũ (vượt biên): phần nến nằm ngoài BB tối thiểu 90% biên độ nến (high-low)

SPIKE_LOOKBACK   = 10   # Số nến gần nhất dùng để tính biên độ/volume trung bình
SPIKE_RANGE_MULT = 7    # Biên độ nến đột biến tối thiểu gấp 7 lần trung bình
SPIKE_VOL_MULT   = 6    # Khối lượng đột biến tối thiểu gấp 6 lần trung bình

MIN_CANDLES_FOR_SIGNAL = BB_PERIOD + 5  # Số nến tối thiểu cần có trước khi bắt đầu xét tín hiệu

ALERT_COOLDOWN_MINUTES        = 30    # Cooldown giữa 2 tín hiệu cùng coin/chiều
LEGACY_ALERT_COOLDOWN_MINUTES = 8 * 60  # Cooldown riêng cho kèo cũ (vượt biên): 8 tiếng, tránh báo liên tục

DOJI_TP_PCT = 0.02     # Chốt lời cố định 2% (kèo Long/Short H1 mới — nến rút râu chạm BB)
DOJI_SL_PCT = 0.015    # Cắt lỗ cố định 1.5% (kèo Long/Short H1 mới)

SPIKE_TP_PCT = 0.025   # Chốt lời cố định 2.5% (kèo Long/Short đột biến — real-time)
SPIKE_SL_PCT = 0.02    # Cắt lỗ cố định 2% (kèo Long/Short đột biến)

LEGACY_TP_PCT = 0.025  # Chốt lời cố định 2.5% (kèo Short H1 cũ)
LEGACY_SL_PCT = 0.02   # Cắt lỗ cố định 2% (kèo Short H1 cũ)

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


def _position_hit(pos: Position, candle: dict) -> Literal["TP", "SL"] | None:
    """Kiểm tra 1 nến (đã đóng hoặc đang hình thành) có chạm TP/SL của lệnh đang mở không."""
    if pos.direction == "SHORT":
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


def detect_legacy_signal(symbol: str, candles: list[dict],
                          direction: Literal["LONG", "SHORT"] = "SHORT") -> Signal | None:
    """Kèo LONG/SHORT H1 cũ (vượt biên) — nến đóng cửa vượt qua BB trên/dưới, với phần nến
    nằm ngoài band tối thiểu LEGACY_BAND_CROSS_MIN_RATIO (90%) tổng biên độ nến."""
    if len(candles) < BB_PERIOD + 2:
        return None

    curr = candles[-1]   # Nến vừa đóng cửa

    rng = curr["high"] - curr["low"]
    if rng <= 0:
        return None

    # BB tính đến trước nến hiện tại, tránh self-reference
    ind = compute_indicators(candles[:-1])
    if ind["bb_middle"] == 0.0:
        return None

    if direction == "LONG":
        # Đóng cửa nằm dưới BB dưới -> báo LONG
        if curr["close"] >= ind["bb_lower"]:
            return None
        outside = min(ind["bb_lower"], curr["high"]) - curr["low"]
        if outside < LEGACY_BAND_CROSS_MIN_RATIO * rng:
            return None
    else:
        # Đóng cửa nằm trên BB trên -> báo SHORT
        if curr["close"] <= ind["bb_upper"]:
            return None
        outside = curr["high"] - max(ind["bb_upper"], curr["low"])
        if outside < LEGACY_BAND_CROSS_MIN_RATIO * rng:
            return None

    logger.info(f"{symbol} {direction} (legacy) | Entry={curr['close']:.4f}")
    return Signal(symbol=symbol, direction=direction, price=curr["close"], sl=0.0, ind=ind)


def detect_spike_signal(symbol: str, closed_candles: list[dict], live_candle: dict,
                         direction: Literal["LONG", "SHORT"] = "SHORT") -> Signal | None:
    """Nến biến động đột biến xuyên BB trên/dưới ngay trong lúc đang hình thành (chưa đóng cửa).
    Báo tín hiệu ngay lập tức — không chờ đóng nến, không chờ rút râu."""
    if len(closed_candles) < max(BB_PERIOD, SPIKE_LOOKBACK):
        return None

    lookback   = closed_candles[-SPIKE_LOOKBACK:]
    avg_range  = sum(c["high"] - c["low"] for c in lookback) / SPIKE_LOOKBACK
    avg_volume = sum(c["volume"] for c in lookback) / SPIKE_LOOKBACK
    if avg_range <= 0 or avg_volume <= 0:
        return None

    live_range = live_candle["high"] - live_candle["low"]
    if live_range < SPIKE_RANGE_MULT * avg_range:
        return None
    if live_candle["volume"] < SPIKE_VOL_MULT * avg_volume:
        return None

    ind = compute_indicators(closed_candles)
    if ind["bb_middle"] == 0.0:
        return None

    if direction == "SHORT":
        if live_candle["high"] <= ind["bb_upper"]:
            return None
    else:
        if live_candle["low"] >= ind["bb_lower"]:
            return None

    logger.info(f"{symbol} {direction} (spike) | Entry={live_candle['close']:.4f}  "
                f"Range={live_range:.4f} (TB={avg_range:.4f})  Vol={live_candle['volume']:.0f} (TB={avg_volume:.0f})")
    return Signal(symbol=symbol, direction=direction, price=live_candle["close"], sl=0.0, ind=ind)

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
        f"*{emoji} {signal.direction} SIGNAL ({interval_display})*\n\n"
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


def _build_legacy_message(signal: Signal, interval_display: str, tp: float) -> str:
    is_short = signal.direction == "SHORT"
    emoji    = "🔴" if is_short else "🟢"
    band     = "trên" if is_short else "dưới"
    return (
        f"*{emoji} {signal.direction} SIGNAL ({interval_display})*\n\n"
        f"Coin: `{signal.symbol}`\n\n"
        f"Điều kiện:\n"
        f"✓ Nến đóng cửa vượt qua BB {band}\n"
        f"✓ Phần nến nằm ngoài band ≥ {LEGACY_BAND_CROSS_MIN_RATIO*100:.0f}% biên độ nến\n\n"
        f"Entry: `{_fmt(signal.price)}`\n"
        f"TP: `{_fmt(tp)}`\n"
        f"SL: `{_fmt(signal.sl)}`"
    )


def _build_spike_message(signal: Signal, interval_display: str, tp: float) -> str:
    is_short = signal.direction == "SHORT"
    emoji    = "🔴🚨" if is_short else "🟢🚨"
    band     = "trên" if is_short else "dưới"
    return (
        f"*{emoji} {signal.direction} SIGNAL - ĐỘT BIẾN ({interval_display})*\n\n"
        f"Coin: `{signal.symbol}`\n\n"
        f"Điều kiện:\n"
        f"✓ Nến đột biến xuyên qua BB {band} (chưa đóng cửa)\n"
        f"✓ Biên độ ≥ {SPIKE_RANGE_MULT} lần trung bình {SPIKE_LOOKBACK} nến\n"
        f"✓ Khối lượng ≥ {SPIKE_VOL_MULT} lần trung bình {SPIKE_LOOKBACK} nến\n"
        f"✓ Báo ngay lập tức, không chờ đóng nến / rút râu\n\n"
        f"Entry: `{_fmt(signal.price)}`\n"
        f"TP: `{_fmt(tp)}`\n"
        f"SL: `{_fmt(signal.sl)}`"
    )


def _build_close_message(pos: Position, interval_display: str, hit: Literal["TP", "SL"]) -> str:
    level  = pos.tp if hit == "TP" else pos.sl
    emoji  = "✅" if hit == "TP" else "🛑"
    pct    = abs(level - pos.entry) / pos.entry * 100
    label  = "Chốt lời (TP)" if hit == "TP" else "Cắt lỗ (SL)"
    return (
        f"*{emoji} {label} — {pos.direction} {pos.symbol} ({interval_display})*\n\n"
        f"Entry: `{_fmt(pos.entry)}`\n"
        f"{hit}: `{_fmt(level)}` (~{pct:.1f}%)"
    )


async def _send_telegram_message(chat_id: str, text: str, tag: str) -> None:
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    logger.info(f"[TG-{tag}] Gửi thành công")
                else:
                    body = await resp.text()
                    logger.error(f"[TG-{tag}] Lỗi {resp.status}: {body}")
    except Exception as e:
        logger.error(f"[TG-{tag}] Không gửi được: {e}")


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
                 cooldown_minutes: int = ALERT_COOLDOWN_MINUTES) -> None:
        self.symbols          = {s.upper() for s in symbols}
        self.interval_display = interval_display
        self.chat_id          = chat_id
        self.detect_fn        = detect_fn
        self.tp_pct           = tp_pct
        self.sl_pct           = sl_pct
        self.message_builder  = message_builder
        self.cooldown_minutes = cooldown_minutes
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
        await send_signal(signal, self.chat_id, self.interval_display, tp, self.message_builder)

    async def on_live_tick(self, symbol: str, candles: list[dict], live_candle: dict) -> None:
        """Check TP/SL real-time theo từng tick giá, không chờ nến đóng."""
        if symbol not in self.symbols:
            return
        await self._check_position(symbol, live_candle)


class SpikeScanner:

    def __init__(self, symbols: list[str], chat_id: str, tp_pct: float, sl_pct: float,
                 executor=None) -> None:
        self.symbols   = {s.upper() for s in symbols}
        self.chat_id   = chat_id
        self.tp_pct    = tp_pct
        self.sl_pct    = sl_pct
        self.executor  = executor   # TradeExecutor | None — nếu có, tự đặt lệnh demo/testnet khi có tín hiệu
        self._last_alert: dict[str, datetime] = {}
        self._last_signal_bar: dict[str, int] = {}   # symbol -> bar_open đã gửi tín hiệu
        self._positions: dict[str, Position] = {}

    def _cooldown_left(self, symbol: str) -> int:
        last = self._last_alert.get(symbol)
        if last is None:
            return 0
        remaining = timedelta(minutes=ALERT_COOLDOWN_MINUTES) - (datetime.now() - last)
        return max(0, int(remaining.total_seconds()))

    async def _check_position(self, symbol: str, candle: dict) -> None:
        pos = self._positions.get(symbol)
        if pos is None:
            return
        hit = _position_hit(pos, candle)
        if hit is None:
            return

        logger.info(f"[SPIKE] {symbol} {pos.direction} {hit} | "
                    f"Entry={pos.entry:.4f}  {hit}={(pos.tp if hit == 'TP' else pos.sl):.4f}")
        if self.executor is None or EXECUTOR_SILENT_DURING_TEST:
            # Bình thường có executor thì lệnh thật đã khớp sẽ tự báo (giá/PnL chính
            # xác hơn ước lượng theo nến này) -> khỏi báo trùng ở đây.
            # (Trong lúc EXECUTOR_SILENT_DURING_TEST=True thì vẫn báo như cũ — xem TODO đầu file.)
            await send_close_alert(pos, self.chat_id, INTERVAL_H1_DISPLAY, hit)
        del self._positions[symbol]

    async def on_closed_candle(self, symbol: str, candles: list[dict]) -> None:
        if symbol not in self.symbols:
            return
        await self._check_position(symbol, candles[-1])

    async def on_live_tick(self, symbol: str, candles: list[dict], live_candle: dict) -> None:
        if symbol not in self.symbols:
            return

        await self._check_position(symbol, live_candle)
        if symbol in self._positions:
            return   # Lệnh của coin này vẫn đang mở, chưa tìm tín hiệu mới

        bar_open = live_candle.get("bar_open")
        if bar_open is not None and self._last_signal_bar.get(symbol) == bar_open:
            return   # Nến này đã có 1 tín hiệu đột biến (Long hoặc Short) gửi rồi — bỏ qua

        signal = (detect_spike_signal(symbol, candles, live_candle, direction="SHORT")
                  or detect_spike_signal(symbol, candles, live_candle, direction="LONG"))
        if signal is None:
            return

        left = self._cooldown_left(symbol)
        if left > 0:
            return

        self._last_alert[symbol] = datetime.now()
        if bar_open is not None:
            self._last_signal_bar[symbol] = bar_open

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

        logger.info(f">>> [SPIKE] TÍN HIỆU: {symbol} {signal.direction} (đột biến) | "
                    f"Entry={signal.price} | TP={tp} | SL={signal.sl}")

        if self.executor is None or EXECUTOR_SILENT_DURING_TEST:
            # Bình thường có executor thì khỏi báo tín hiệu "dự đoán" song song ở đây —
            # executor tự báo bằng giá khớp THẬT sau khi đặt lệnh (tránh trùng/nhiễu).
            # (Trong lúc EXECUTOR_SILENT_DURING_TEST=True thì vẫn báo như cũ — xem TODO đầu file.)
            await send_signal(signal, self.chat_id, INTERVAL_H1_DISPLAY, tp, _build_spike_message)

        if self.executor is not None:
            # Ưu tiên: đặt lệnh lên Binance TRƯỚC, executor tự lấy giá khớp THẬT rồi
            # mới báo (notify trong executor.py — hiện chỉ log do EXECUTOR_SILENT_DURING_TEST).
            asyncio.create_task(self.executor.open_position(
                symbol=symbol, direction=signal.direction,
                entry_price=signal.price, sl_price=signal.sl, tp_price=tp,
                sl_pct=self.sl_pct, tp_pct=self.tp_pct,
            ))

# ═══════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════
def _banner() -> None:
    logger.info("=" * 50)
    logger.info("  Binance Futures Song Kiem Signal Bot (WebSocket real-time)")
    logger.info("=" * 50)
    if AUTO_TOP_SYMBOLS:
        logger.info(f"  Symbol    : Tự động top {TOP_SYMBOLS_COUNT} (LONG/SHORT) | top {LEGACY_TOP_SYMBOLS_COUNT} (LONG/SHORT cũ)")
    else:
        logger.info(f"  Symbol    : Thủ công {len(SYMBOLS)} cặp")
    logger.info(f"  Timeframe : {INTERVAL_H1_DISPLAY}  (chạy cho cả 6 kèo, 1 LiveFeed dùng chung)")
    logger.info(f"  Nguồn nến : Futures WebSocket (path /market)")
    logger.info(f"  LONG/SHORT mới (chạm BB)   -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID else 'OK'}  "
                f"TP/SL={DOJI_TP_PCT*100:.1f}%/{DOJI_SL_PCT*100:.1f}%")
    logger.info(f"  LONG/SHORT đột biến (real-time) -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID_SPIKE else 'OK'}  "
                f"TP/SL={SPIKE_TP_PCT*100:.1f}%/{SPIKE_SL_PCT*100:.1f}%  "
                f"range>={SPIKE_RANGE_MULT}x  vol>={SPIKE_VOL_MULT}x")
    logger.info(f"  LONG/SHORT cũ (vượt BB)    -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID_H1 else 'OK'}  "
                f"TP/SL={LEGACY_TP_PCT*100:.1f}%/{LEGACY_SL_PCT*100:.1f}%")
    logger.info(f"  BB        : period={BB_PERIOD}  std={BB_STD}")
    logger.info(f"  Cooldown  : {ALERT_COOLDOWN_MINUTES} phút (mới/đột biến)  |  "
                f"{LEGACY_ALERT_COOLDOWN_MINUTES // 60} tiếng (cũ)")
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
    await _check_telegram_connection(TELEGRAM_CHAT_ID_H1, "LEGACY-SHORT-H1")
    await _check_telegram_connection(TELEGRAM_CHAT_ID_SPIKE, "SPIKE")


async def _run_forever(label: str, feed: LiveFeed) -> None:
    """Chạy LiveFeed vô hạn; nếu crash bất ngờ (hiếm, vì LiveFeed đã tự retry nội bộ)
    thì log + khởi động lại thay vì để cả bot dừng hẳn."""
    while True:
        try:
            await feed.run()
        except Exception as e:
            logger.critical(f"[{label}] LiveFeed dừng bất ngờ, khởi động lại sau 10s: {e}", exc_info=True)
            await asyncio.sleep(10)


async def _main() -> None:
    _banner()
    await _check_telegram_connections()
    try:
        symbols        = await resolve_symbols(TOP_SYMBOLS_COUNT)
        legacy_symbols = await resolve_symbols(LEGACY_TOP_SYMBOLS_COUNT)
        all_symbols    = sorted(set(symbols) | set(legacy_symbols))

        feed = LiveFeed(all_symbols, INTERVAL_H1, CANDLE_BUFFER)

        long_scanner = Scanner(
            symbols, INTERVAL_H1_DISPLAY, TELEGRAM_CHAT_ID,
            detect_fn=lambda s, c: detect_signal(s, c, direction="LONG"),
            tp_pct=DOJI_TP_PCT, sl_pct=DOJI_SL_PCT,
        )
        short_scanner = Scanner(
            symbols, INTERVAL_H1_DISPLAY, TELEGRAM_CHAT_ID,
            detect_fn=lambda s, c: detect_signal(s, c, direction="SHORT"),
            tp_pct=DOJI_TP_PCT, sl_pct=DOJI_SL_PCT,
        )
        legacy_long_scanner = Scanner(
            legacy_symbols, INTERVAL_H1_DISPLAY, TELEGRAM_CHAT_ID_H1,
            detect_fn=lambda s, c: detect_legacy_signal(s, c, direction="LONG"),
            tp_pct=LEGACY_TP_PCT, sl_pct=LEGACY_SL_PCT,
            message_builder=_build_legacy_message,
            cooldown_minutes=LEGACY_ALERT_COOLDOWN_MINUTES,
        )
        legacy_short_scanner = Scanner(
            legacy_symbols, INTERVAL_H1_DISPLAY, TELEGRAM_CHAT_ID_H1,
            detect_fn=lambda s, c: detect_legacy_signal(s, c, direction="SHORT"),
            tp_pct=LEGACY_TP_PCT, sl_pct=LEGACY_SL_PCT,
            cooldown_minutes=LEGACY_ALERT_COOLDOWN_MINUTES,
            message_builder=_build_legacy_message,
        )
        executor = None
        if ENABLE_AUTO_TRADE and TradeExecutor is not None:
            # notify_always: LUÔN gửi Telegram thật, kể cả lúc EXECUTOR_SILENT_DURING_TEST
            # đang bật — dùng cho cảnh báo cần thấy ngay (vd: symbol không giao dịch được
            # trên Demo Trading), khác với notify (báo kết quả lệnh, tôn trọng cờ silent).
            notify_always = lambda text: _send_telegram_message(TELEGRAM_CHAT_ID_SPIKE, text, "EXECUTOR-INFO")
            if EXECUTOR_SILENT_DURING_TEST:
                # TODO(demo-test): chỉ ghi log, không gửi Telegram — xem TODO đầu file.
                async def notify(text: str) -> None:
                    logger.info(f"[Executor-DEMO] {text}")
            else:
                notify = lambda text: _send_telegram_message(TELEGRAM_CHAT_ID_SPIKE, text, "EXECUTOR")
            executor = await TradeExecutor.create(notify, notify_always)
            logger.warning("[Executor] AUTO-TRADE ĐANG BẬT — bot sẽ tự đặt lệnh (xem executor.py để kiểm tra testnet/thật)")
            await executor.reconcile_on_startup(set(symbols))

        spike_scanner = SpikeScanner(
            symbols, TELEGRAM_CHAT_ID_SPIKE, tp_pct=SPIKE_TP_PCT, sl_pct=SPIKE_SL_PCT,
            executor=executor,
        )

        for sc in (long_scanner, short_scanner, legacy_long_scanner, legacy_short_scanner,
                   spike_scanner):
            feed.on_closed_candle(sc.on_closed_candle)
            feed.on_live_tick(sc.on_live_tick)

        if executor is not None:
            await asyncio.gather(
                _run_forever("LiveFeed", feed),
                executor.run_user_data_stream(),
                executor.run_reconciliation_loop(set(symbols)),
            )
        else:
            await _run_forever("LiveFeed", feed)
    except KeyboardInterrupt:
        logger.info("Bot dừng.")
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(_main())
