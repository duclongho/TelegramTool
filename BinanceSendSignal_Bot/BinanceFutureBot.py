import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Literal, TypedDict
from urllib.parse import urlencode

import aiohttp

TELEGRAM_TOKEN         = "8641278115:AAEB08VXrX5YJl_2zzM_SFF4JRdEwIfAj-s"
TELEGRAM_CHAT_ID       = "-1004448248877"
TELEGRAM_CHAT_ID_H1    = "-1004340326145"
TELEGRAM_CHAT_ID_PIVOT = "-1004479723244"

KEO_RUTRAU_NAME   = "BB H1 Rút Râu"
KEO_LEGACY_NAME   = "RSI H4 Đảo Biên"
KEO_PIVOT_NAME    = "Pivot DCA Đảo Chiều"

AUTO_TOP_SYMBOLS  = True
TOP_SYMBOLS_COUNT = 200
LEGACY_TOP_SYMBOLS_COUNT = 150

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

PIVOT_SYMBOLS_M5 = [
    "XAUUSDT",
]
PIVOT_SYMBOLS_M15 = [
    "BNBUSDT",
    "ETHUSDT",
    "BTCUSDT",
]
PIVOT_SYMBOLS = PIVOT_SYMBOLS_M5 + PIVOT_SYMBOLS_M15

CANDLE_BUFFER = 150

INTERVAL_H1         = "1h"
INTERVAL_H1_DISPLAY = "H1"

INTERVAL_H4         = "4h"
INTERVAL_H4_DISPLAY = "H4"

INTERVAL_M15         = "15m"
INTERVAL_M15_DISPLAY = "M15"

INTERVAL_M5          = "5m"
INTERVAL_M5_DISPLAY  = "M5"

BB_PERIOD = 20
BB_STD    = 2.0

DOJI_BODY_MAX_RATIO       = 0.3
DOJI_SHORT_WICK_MAX_RATIO = 0.1
BAND_CROSS_MIN_RATIO      = 0.1

MIN_CANDLES_FOR_SIGNAL = BB_PERIOD + 5

ALERT_COOLDOWN_MINUTES        = 30

DOJI_TP_PCT = 0.02
DOJI_SL_PCT = 0.015

LEGACY_RSI_PERIOD       = 6
LEGACY_RSI_OVERBOUGHT   = 90
LEGACY_RSI_SHORT_CONFIRM = 75
LEGACY_RSI_OVERSOLD     = 70
LEGACY_RSI_LONG_CONFIRM = 75
LEGACY_TP_PCT = 0.03
LEGACY_SL_PCT = 0.03

PIVOT_LENGTH  = 50
PIVOT_MAX_DCA = 2
PIVOT_CANDLE_BUFFER = 500

PIVOT_LIVE_TRADING  = True
BINANCE_API_KEY     = "FPaPRx6ECzzQ25RgVqjozp3qsFMrc5u3vQScdfecy7oZUEs7UKUcdjdixtb03uNI"
BINANCE_API_SECRET  = "t7MqGdFRTAyVLmRhzRn7o3ixHEU83YnJzqsQUsPpxfkiktqsOQ7crQSisCqM6FYw"
PIVOT_LEVERAGE      = 50
PIVOT_MARGIN_TYPE   = "CROSSED"
PIVOT_ACCOUNT_PCT   = 0.05
PIVOT_FIXED_MARGIN_USDT = {
    "XAUUSDT": 5.0,
}

WS_MAX_STREAMS_PER_CONN = 190
WS_RECONNECT_DELAY_SEC  = 5
WS_HEARTBEAT_MINUTES    = 60
WS_NO_DATA_TIMEOUT_SEC  = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

class Indicators(TypedDict):
    bb_upper:  float
    bb_middle: float
    bb_lower:  float


def _calc_bb(closes: list[float], period: int, multiplier: float) -> tuple[float, float, float]:
    if len(closes) < period:
        return 0.0, 0.0, 0.0
    window   = closes[-period:]
    middle   = sum(window) / period
    variance = sum((x - middle) ** 2 for x in window) / period
    std      = variance ** 0.5
    return middle + multiplier * std, middle, middle - multiplier * std


def compute_indicators(candles: list[dict]) -> Indicators:
    closes = [c["close"] for c in candles]
    bb_upper, bb_middle, bb_lower = _calc_bb(closes, BB_PERIOD, BB_STD)
    return Indicators(bb_upper=bb_upper, bb_middle=bb_middle, bb_lower=bb_lower)


def _calc_rsi(closes: list[float], period: int) -> list[float]:
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
    bar_open: int
    price:    float
    type:     Literal["H", "L"]


def _find_swing_points(candles: list[dict], k: int, count: int = 3) -> list[SwingPoint] | None:
    n = len(candles)
    last_confirmable = n - 1 - k
    if last_confirmable < k:
        return None

    zigzag: list[SwingPoint] = []
    for idx in range(k, last_confirmable + 1):
        bar_open = candles[idx].get("bar_open")
        if bar_open is None:
            continue
        before = candles[idx - k:idx]
        after  = candles[idx + 1:idx + 1 + k]
        hi, lo = candles[idx]["high"], candles[idx]["low"]
        is_high = all(hi > c["high"] for c in before) and all(hi > c["high"] for c in after)
        is_low  = all(lo < c["low"]  for c in before) and all(lo < c["low"]  for c in after)
        if not (is_high or is_low):
            continue
        ptype = "H" if is_high else "L"
        price = hi if is_high else lo
        point: SwingPoint = {"bar_open": bar_open, "price": price, "type": ptype}

        if zigzag and zigzag[-1]["type"] == ptype:
            more_extreme = (price > zigzag[-1]["price"]) if ptype == "H" else (price < zigzag[-1]["price"])
            if more_extreme:
                zigzag[-1] = point
        else:
            zigzag.append(point)

    if len(zigzag) < count:
        return None
    return zigzag[-count:]


@dataclass
class Signal:
    symbol:    str
    direction: Literal["LONG", "SHORT"]
    price:     float
    sl:        float
    ind:       Indicators


@dataclass
class Position:
    symbol:    str
    direction: Literal["LONG", "SHORT"]
    entry:     float
    tp:        float
    sl:        float
    opened_at: datetime
    entry_bar_open: int | None = None


def _position_hit(pos: Position, candle: dict) -> Literal["TP", "SL"] | None:
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
        bearish = candle["close"] <= candle["open"]
        return ("TP" if bearish else "SL") if pos.direction == "SHORT" else ("SL" if bearish else "TP")
    return "TP" if hit_tp else "SL"


class DailyStats:

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

    prev = candles[-2]
    curr = candles[-1]

    rng = curr["high"] - curr["low"]
    if rng <= 0:
        return None

    body = abs(curr["close"] - curr["open"])
    if body > DOJI_BODY_MAX_RATIO * rng:
        return None

    if curr["volume"] < 2 * prev["volume"]:
        return None

    ind = compute_indicators(candles[:-1])
    if ind["bb_middle"] == 0.0:
        return None

    if direction == "LONG":
        if not (curr["close"] > curr["open"]):
            return None
        upper_wick = curr["high"] - max(curr["open"], curr["close"])
        if upper_wick > DOJI_SHORT_WICK_MAX_RATIO * rng:
            return None
        if ind["bb_lower"] - curr["low"] < BAND_CROSS_MIN_RATIO * rng:
            return None
        if curr["high"] >= ind["bb_middle"]:
            return None
    else:
        if not (curr["close"] < curr["open"]):
            return None
        lower_wick = min(curr["open"], curr["close"]) - curr["low"]
        if lower_wick > DOJI_SHORT_WICK_MAX_RATIO * rng:
            return None
        if curr["high"] - ind["bb_upper"] < BAND_CROSS_MIN_RATIO * rng:
            return None
        if curr["low"] <= ind["bb_middle"]:
            return None

    logger.info(f"{symbol} {direction} | Entry={curr['close']:.4f}")
    return Signal(symbol=symbol, direction=direction, price=curr["close"], sl=0.0, ind=ind)





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
                    else f"đã ở dưới {armed_level} rồi đi lên tới {fire_level}")
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


TELEGRAM_SEND_RETRIES     = 3
TELEGRAM_SEND_RETRY_DELAY = 2.0

TELEGRAM_DEDUPE_WINDOW_SEC = 180
_recent_sends: dict[tuple[str, str], datetime] = {}


async def _send_telegram_message(chat_id: str, text: str, tag: str) -> None:
    key = (chat_id, text)
    now = datetime.now()
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


_FUTURES_REST = "https://fapi.binance.com"
_FUTURES_WS   = "wss://fstream.binance.com"


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

    def __init__(self, symbols: list[str], interval: str, buffer_size: int, name: str | None = None) -> None:
        self.symbols     = sorted({s.upper() for s in symbols})
        self.interval    = interval
        self.buffer_size = buffer_size
        self.name        = name or interval
        self.candles: dict[str, deque] = defaultdict(lambda: deque(maxlen=buffer_size))
        self._last_close: dict[str, int] = {}
        self._closed_handlers: list[Callable[[str, list[dict]], Awaitable[None]]] = []
        self._live_handlers: list[Callable[[str, list[dict], dict], Awaitable[None]]] = []
    def on_closed_candle(self, handler: Callable[[str, list[dict]], Awaitable[None]]) -> None:
        self._closed_handlers.append(handler)

    def on_live_tick(self, handler: Callable[[str, list[dict], dict], Awaitable[None]]) -> None:
        self._live_handlers.append(handler)

    async def _fetch_history(self, symbols: list[str]) -> None:
        logger.info(f"[LiveFeed-{self.name}] Nạp lịch sử {len(symbols)} coin...")
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
                            for k in rows[:-1]:
                                self.candles[sym].append({
                                    "open": float(k[1]), "high": float(k[2]),
                                    "low":  float(k[3]), "close": float(k[4]),
                                    "volume": float(k[5]),
                                    "bar_open": int(k[0]),
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
        logger.info(f"[LiveFeed-{self.name}] Nạp xong {ok}/{len(symbols)}" +
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
            "bar_open": int(k["t"]),
        }
        if len(self.candles[symbol]) < MIN_CANDLES_FOR_SIGNAL:
            return

        if k["x"]:
            close_time = int(k["T"])
            if self._last_close.get(symbol) == close_time:
                return
            self._last_close[symbol] = close_time
            self.candles[symbol].append(candle)
            await self._dispatch_closed(symbol, list(self.candles[symbol]))
        else:
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
                        logger.info(f"[LiveFeed-{self.name}] WS bắt tay thành công ({len(chunk)} coin), "
                                    f"đang chờ dữ liệu đầu tiên (timeout {WS_NO_DATA_TIMEOUT_SEC}s)...")
                        while True:
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=WS_NO_DATA_TIMEOUT_SEC)
                            except asyncio.TimeoutError:
                                print(f"⚠️ [LiveFeed-{self.name}] KHÔNG nhận được bất kỳ dữ liệu nào trong "
                                      f"{WS_NO_DATA_TIMEOUT_SEC}s ({len(chunk)} coin) — kết nối lại...")
                                logger.warning(f"[LiveFeed-{self.name}] Timeout không có dữ liệu "
                                               f"({WS_NO_DATA_TIMEOUT_SEC}s, {len(chunk)} coin), kết nối lại")
                                raise ConnectionError("Không nhận được dữ liệu — timeout watchdog")

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                payload = json.loads(msg.data)
                                data = payload.get("data", payload)
                                k = data.get("k")
                                if k is None:
                                    logger.info(f"[LiveFeed-{self.name}] Nhận message không phải kline: "
                                                f"{str(msg.data)[:200]}")
                                    continue
                                msg_count += 1
                                if first_message:
                                    first_message = False
                                    print(f"✅ [LiveFeed-{self.name}] Đã NHẬN được dữ liệu real-time từ Binance "
                                          f"({len(chunk)} coin) — mẫu: {data['s']} close={k['c']} "
                                          f"đóng={k['x']}")
                                    logger.info(f"[LiveFeed-{self.name}] Xác nhận nhận dữ liệu real-time OK "
                                                f"({len(chunk)} coin) — mẫu: {data['s']} close={k['c']}")

                                now = datetime.now()
                                if now - last_heartbeat >= timedelta(minutes=WS_HEARTBEAT_MINUTES):
                                    last_heartbeat = now
                                    print(f"✅ [LiveFeed-{self.name}] Vẫn đang kết nối Binance OK "
                                          f"({len(chunk)} coin, đã nhận {msg_count} update) — "
                                          f"mẫu: {data['s']} close={k['c']}")
                                    logger.info(f"[LiveFeed-{self.name}] Heartbeat — vẫn kết nối OK "
                                                f"({len(chunk)} coin, {msg_count} update đã nhận)")

                                await self._handle_kline_event(data["s"], k)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE,
                                               aiohttp.WSMsgType.ERROR):
                                raise ConnectionError(f"WS đóng/lỗi: {msg}")
                            else:
                                logger.info(f"[LiveFeed-{self.name}] Nhận message loại khác: "
                                            f"{msg.type} — data={str(msg.data)[:200]}")
            except Exception as e:
                logger.error(f"[LiveFeed-{self.name}] WS lỗi ({len(chunk)} coin), "
                             f"đồng bộ lại + kết nối lại sau {WS_RECONNECT_DELAY_SEC}s: {e}")
                try:
                    await self._fetch_history(chunk)
                except Exception as e2:
                    logger.error(f"[LiveFeed-{self.name}] Đồng bộ lại thất bại: {e2}")
                await asyncio.sleep(WS_RECONNECT_DELAY_SEC)

    async def run(self) -> None:
        await self._fetch_history(self.symbols)
        chunks = [self.symbols[i:i + WS_MAX_STREAMS_PER_CONN]
                  for i in range(0, len(self.symbols), WS_MAX_STREAMS_PER_CONN)]
        logger.info(f"[LiveFeed-{self.name}] Mở {len(chunks)} kết nối WS cho {len(self.symbols)} coin")
        await asyncio.gather(*(self._run_connection(c) for c in chunks))

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
        self.daily_stats      = daily_stats
        self._last_alert: dict[str, datetime] = {}
        self._positions: dict[str, Position] = {}

    def _cooldown_left(self, symbol: str, direction: str) -> int:
        last = self._last_alert.get(f"{symbol}_{direction}")
        if last is None:
            return 0
        remaining = timedelta(minutes=self.cooldown_minutes) - (datetime.now() - last)
        return max(0, int(remaining.total_seconds()))

    async def _check_position(self, symbol: str, candle: dict) -> None:
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
            return

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
        if symbol not in self.symbols:
            return
        await self._check_position(symbol, live_candle)




class RsiExtremeScanner:

    def __init__(self, symbols: list[str], chat_id: str,
                 daily_stats: "DailyStats | None" = None) -> None:
        self.symbols     = {s.upper() for s in symbols}
        self.chat_id     = chat_id
        self.daily_stats = daily_stats
        self._last_fired: dict[tuple[str, str], datetime] = {}
        self._positions: dict[str, Position] = {}
        self._bar_open:    dict[str, int]  = {}
        self._short_armed: dict[str, bool] = {}
        self._long_armed:  dict[str, bool] = {}

    def _already_fired_today(self, symbol: str, direction: Literal["LONG", "SHORT"]) -> bool:
        last = self._last_fired.get((symbol, direction))
        return last is not None and last.date() == datetime.now().date()

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
        if bar_open is None or self._bar_open.get(symbol) == bar_open:
            return
        self._bar_open[symbol]    = bar_open
        self._short_armed[symbol] = False
        self._long_armed[symbol]  = False

    async def _fire(self, symbol: str, direction: Literal["LONG", "SHORT"],
                     price: float, rsi: float, bar_open: int | None) -> None:
        if symbol in self._positions:
            return
        if self._already_fired_today(symbol, direction):
            logger.info(f"[H4-RSI] {symbol} {direction}: đã báo hôm nay rồi, chờ sang ngày mới")
            return
        self._last_fired[(symbol, direction)] = datetime.now()

        empty_ind: Indicators = {"bb_upper": 0.0, "bb_middle": 0.0, "bb_lower": 0.0}
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
            entry_bar_open=bar_open,
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
            self._short_armed[symbol] = False
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
            await self._check_signal(symbol, candles[:-1], candles[-1])

    async def on_live_tick(self, symbol: str, candles: list[dict], live_candle: dict) -> None:
        if symbol not in self.symbols:
            return
        await self._check_position(symbol, live_candle)
        await self._check_signal(symbol, candles, live_candle)


@dataclass
class PivotSignal:
    bar_open: int | None
    high:     float
    low:      float


def _detect_pivot(candles: list[dict], length: int) -> tuple["PivotSignal | None", "PivotSignal | None"]:
    n = len(candles)
    idx = n - 1 - length
    if idx - length < 0:
        return None, None

    pivot  = candles[idx]
    before = candles[idx - length:idx]
    after  = candles[idx + 1:n]

    is_high = all(pivot["high"] > c["high"] for c in before) and all(pivot["high"] > c["high"] for c in after)
    is_low  = all(pivot["low"]  < c["low"]  for c in before) and all(pivot["low"]  < c["low"]  for c in after)

    ph = PivotSignal(bar_open=pivot.get("bar_open"), high=pivot["high"], low=pivot["low"]) if is_high else None
    pl = PivotSignal(bar_open=pivot.get("bar_open"), high=pivot["high"], low=pivot["low"]) if is_low else None
    return ph, pl


def _detect_missed_pivot(candles: list[dict], length: int) -> tuple["PivotSignal | None", "PivotSignal | None"]:
    n_total = len(candles)
    if n_total < 2 * length + 1:
        return None, None

    max_ = min_ = follow_max = follow_min = 0.0
    max_c = min_c = follow_max_c = follow_min_c = None
    os_ = 0
    initialized = False
    missed_high = missed_low = None

    for n in range(n_total):
        pivot_idx = n - length
        if pivot_idx - length < 0:
            continue
        before = candles[pivot_idx - length:pivot_idx]
        after  = candles[pivot_idx + 1:n + 1]
        if len(before) < length or len(after) < length:
            continue
        pb = candles[pivot_idx]
        hi_len, lo_len = pb["high"], pb["low"]

        if not initialized:
            max_ = follow_max = hi_len
            min_ = follow_min = lo_len
            max_c = follow_max_c = min_c = follow_min_c = pb
            initialized = True
            prev_os = os_
        else:
            prev_max, prev_min = max_, min_
            prev_follow_max, prev_follow_min = follow_max, follow_min
            prev_os = os_

            max_ = max(hi_len, max_)
            min_ = min(lo_len, min_)
            follow_max = max(hi_len, follow_max)
            follow_min = min(lo_len, follow_min)

            if max_ > prev_max:
                max_c = pb
                follow_min, follow_min_c = lo_len, pb
            if min_ < prev_min:
                min_c = pb
                follow_max, follow_max_c = hi_len, pb

            if follow_min < prev_follow_min:
                follow_min_c = pb
            if follow_max > prev_follow_max:
                follow_max_c = pb

        is_high = all(pb["high"] > c["high"] for c in before) and all(pb["high"] > c["high"] for c in after)
        is_low  = all(pb["low"]  < c["low"]  for c in before) and all(pb["low"]  < c["low"]  for c in after)

        this_mh = this_ml = None

        if is_high:
            ph_val = pb["high"]
            if prev_os == 1:
                this_ml = min_c
            elif ph_val < max_:
                this_mh = max_c
                this_ml = follow_min_c
            os_ = 1
            max_ = min_ = ph_val

        if is_low:
            pl_val = pb["low"]
            if prev_os == 0:
                this_mh = max_c
            elif pl_val > min_:
                this_mh = follow_max_c
                this_ml = min_c
            os_ = 0
            max_ = min_ = pl_val

        if n == n_total - 1:
            missed_high = PivotSignal(this_mh.get("bar_open"), this_mh["high"], this_mh["low"]) if this_mh else None
            missed_low  = PivotSignal(this_ml.get("bar_open"), this_ml["high"], this_ml["low"]) if this_ml else None

    return missed_high, missed_low


def _detect_running_extreme(candles: list[dict], length: int) -> tuple["PivotSignal | None", "PivotSignal | None"]:
    n_total = len(candles)
    if n_total < 2 * length + 1:
        return None, None

    os_ = 0
    px1_idx = 0

    for n in range(n_total):
        pivot_idx = n - length
        if pivot_idx - length < 0:
            continue
        before = candles[pivot_idx - length:pivot_idx]
        after  = candles[pivot_idx + 1:n + 1]
        if len(before) < length or len(after) < length:
            continue
        pb = candles[pivot_idx]
        is_high = all(pb["high"] > c["high"] for c in before) and all(pb["high"] > c["high"] for c in after)
        is_low  = all(pb["low"]  < c["low"]  for c in before) and all(pb["low"]  < c["low"]  for c in after)
        if is_high:
            os_ = 1
            px1_idx = pivot_idx
        if is_low:
            os_ = 0
            px1_idx = pivot_idx

    start = px1_idx + 1
    if start > n_total - 1:
        return None, None

    window = candles[start:n_total]
    last = candles[-1]
    if os_ == 1:
        extreme_low = min(c["low"] for c in window)
        if last["low"] <= extreme_low:
            return None, PivotSignal(last.get("bar_open"), last["high"], last["low"])
        return None, None
    else:
        extreme_high = max(c["high"] for c in window)
        if last["high"] >= extreme_high:
            return PivotSignal(last.get("bar_open"), last["high"], last["low"]), None
        return None, None


@dataclass
class PivotEntry:
    price:    float
    bar_open: int | None
    qty:      float = 0.0


@dataclass
class PivotPosition:
    direction: Literal["LONG", "SHORT"]
    entries:   list[PivotEntry]
    margin_per_entry: float = 0.0

    @property
    def avg_entry(self) -> float:
        return sum(e.price for e in self.entries) / len(self.entries)

    @property
    def total_qty(self) -> float:
        return sum(e.qty for e in self.entries)


def _build_pivot_open_message(symbol: str, pos: PivotPosition, kind: str, order_err: str | None = None) -> str:
    is_short = pos.direction == "SHORT"
    emoji    = "🔴" if is_short else "🟢"
    lines = [
        f"*{emoji} {pos.direction} — {KEO_PIVOT_NAME}*",
        "",
        f"Coin: `{symbol}`",
        f"Hành động: *{kind}* (lệnh {len(pos.entries)}/{PIVOT_MAX_DCA + 1})",
        "",
    ]
    for i, e in enumerate(pos.entries, 1):
        qty_note = f"  ·  KL `{e.qty}` ({PIVOT_LEVERAGE}x)" if e.qty > 0 else ""
        lines.append(f"Entry {i}: `{_fmt(e.price)}`{qty_note}")
    lines.append(f"Giá vào TB: `{_fmt(pos.avg_entry)}`")
    if order_err:
        lines.append(f"\n⚠️ *ĐẶT LỆNH THẬT THẤT BẠI* — chỉ ghi nhận tín hiệu, KHÔNG có lệnh thật trên sàn:\n`{order_err}`")
    return "\n".join(lines)


def _build_pivot_close_message(symbol: str, pos: PivotPosition, exit_price: float,
                                pnl_pct: float, reason: str, order_err: str | None = None) -> str:
    emoji = "✅" if pnl_pct >= 0 else "🛑"
    lines = [
        f"*{emoji} ĐÓNG TẤT CẢ {pos.direction} — {KEO_PIVOT_NAME}*",
        "",
        f"Coin: `{symbol}`",
        f"Lý do: {reason}",
        f"Số lệnh: *{len(pos.entries)}*",
        f"Giá vào TB: `{_fmt(pos.avg_entry)}`",
        f"Giá đóng: `{_fmt(exit_price)}`",
        f"PnL ước tính: *{pnl_pct:+.2f}%*",
    ]
    if pos.total_qty > 0:
        lines.append(f"Tổng khối lượng đã đóng: `{pos.total_qty}`")
    if order_err:
        lines.append(f"\n⚠️ *ĐÓNG LỆNH THẬT THẤT BẠI* — vị thế trên sàn có thể VẪN CÒN MỞ, cần tự kiểm tra:\n`{order_err}`")
    return "\n".join(lines)


class BinanceExecutor:

    def __init__(self, api_key: str, api_secret: str, leverage: int, margin_type: str,
                 account_pct: float, entries_per_position: int,
                 fixed_margin_usdt: dict[str, float] | None = None) -> None:
        self.api_key              = api_key
        self.api_secret           = api_secret
        self.leverage             = leverage
        self.margin_type          = margin_type
        self.account_pct          = account_pct
        self.entries_per_position = entries_per_position
        self.fixed_margin_usdt    = {s.upper(): v for s, v in (fixed_margin_usdt or {}).items()}
        self._qty_precision: dict[str, int] = {}
        self._time_offset_ms = 0

    async def sync_time(self) -> None:
        local_before = int(time.time() * 1000)
        data = await self._request("GET", "v1/time", signed=False)
        local_after = int(time.time() * 1000)
        server_time = int(data["serverTime"])
        local_mid = (local_before + local_after) // 2   # trừ hao độ trễ round-trip của request này
        self._time_offset_ms = server_time - local_mid
        logger.info(f"[Pivot Executor] Đồng bộ giờ với Binance — máy lệch {self._time_offset_ms:+d}ms")

    def _sign(self, params: dict) -> dict:
        params = dict(params)
        params["timestamp"]  = int(time.time() * 1000) + self._time_offset_ms
        params["recvWindow"] = 10000
        query = urlencode(params)
        params["signature"] = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return params

    async def _request(self, method: str, path: str, params: dict | None = None, signed: bool = True) -> dict:
        params = self._sign(params or {}) if signed else (params or {})
        url = f"{_FUTURES_REST}/fapi/{path}"
        headers = {"X-MBX-APIKEY": self.api_key} if signed else {}
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.request(method, url, params=params, headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=10)) as resp:
                raw = await resp.text()
                try:
                    data = json.loads(raw)
                except Exception:
                    raise RuntimeError(f"HTTP {resp.status} {path} params={params}: "
                                        f"phản hồi không phải JSON (có thể sai đường dẫn API): {raw[:300]}")
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} {path} params={params}: {data}")
                return data

    async def setup_symbol(self, symbol: str) -> None:
        info = await self._request("GET", "v1/exchangeInfo", signed=False)
        for s in info.get("symbols", []):
            if s["symbol"] == symbol:
                self._qty_precision[symbol] = s["quantityPrecision"]
                break
        else:
            raise RuntimeError(f"Không tìm thấy symbol {symbol} trong exchangeInfo")

        try:
            await self._request("POST", "v1/marginType", {"symbol": symbol, "marginType": self.margin_type})
        except RuntimeError as e:
            if "-4046" in str(e):
                pass
            elif "-4048" in str(e):
                logger.warning(f"[Pivot Executor] {symbol}: đang có vị thế mở từ trước nên KHÔNG đổi "
                                f"được margin type sang {self.margin_type} — giữ nguyên margin type "
                                f"hiện tại trên sàn cho coin này, các bước setup khác vẫn tiếp tục")
            else:
                raise
        await self._request("POST", "v1/leverage", {"symbol": symbol, "leverage": self.leverage})

    async def get_available_balance_usdt(self) -> float:
        data = await self._request("GET", "v2/balance", {}, signed=True)
        for asset in data:
            if asset["asset"] == "USDT":
                return float(asset["availableBalance"])
        return 0.0

    async def compute_entry_margin(self, symbol: str) -> float:
        fixed = self.fixed_margin_usdt.get(symbol.upper())
        if fixed is not None:
            return fixed
        balance = await self.get_available_balance_usdt()
        return (balance * self.account_pct) / self.entries_per_position

    def _round_qty(self, symbol: str, qty: float) -> float:
        precision = self._qty_precision.get(symbol, 3)
        factor = 10 ** precision
        return math.floor(qty * factor) / factor

    def _fmt_qty(self, symbol: str, qty: float) -> str:
        precision = self._qty_precision.get(symbol, 3)
        return f"{qty:.{precision}f}"

    async def market_order(self, symbol: str, side: str, margin_usdt: float, price_hint: float) -> float:
        notional = margin_usdt * self.leverage
        qty = self._round_qty(symbol, notional / price_hint)
        if qty <= 0:
            raise RuntimeError(f"Khối lượng tính ra = 0 (margin={margin_usdt:.2f} USDT quá nhỏ so với giá/đòn bẩy)")
        data = await self._request("POST", "v1/order", {
            "symbol": symbol, "side": side, "type": "MARKET",
            "quantity": self._fmt_qty(symbol, qty), "newOrderRespType": "RESULT",
        })
        return float(data.get("executedQty", qty))

    async def close_position(self, symbol: str, side: str, qty: float) -> None:
        qty = self._round_qty(symbol, qty)
        if qty <= 0:
            return
        await self._request("POST", "v1/order", {
            "symbol": symbol, "side": side, "type": "MARKET",
            "quantity": self._fmt_qty(symbol, qty), "reduceOnly": "true", "newOrderRespType": "RESULT",
        })


class PivotDcaScanner:

    def __init__(self, symbols: list[str], chat_id: str, length: int = PIVOT_LENGTH,
                 max_dca: int = PIVOT_MAX_DCA, daily_stats: "DailyStats | None" = None,
                 executor: "BinanceExecutor | None" = None,
                 dca_confirm_symbols: set[str] | None = None) -> None:
        self.symbols     = {s.upper() for s in symbols}
        self.chat_id     = chat_id
        self.length      = length
        self.max_dca     = max_dca
        self.daily_stats = daily_stats
        self.executor    = executor
        self.dca_confirm_symbols = {s.upper() for s in (dca_confirm_symbols or set())}
        self._armed_long:  dict[str, PivotSignal]   = {}
        self._armed_short: dict[str, PivotSignal]   = {}
        self._position:    dict[str, PivotPosition] = {}

    async def _close_all(self, symbol: str, price: float, reason: str) -> None:
        pos = self._position.pop(symbol, None)
        if pos is None:
            return

        order_err = None
        if self.executor is not None and pos.total_qty > 0:
            side = "SELL" if pos.direction == "LONG" else "BUY"
            try:
                await self.executor.close_position(symbol, side, pos.total_qty)
            except Exception as e:
                order_err = str(e)
                logger.error(f"[{KEO_PIVOT_NAME}] {symbol} ĐÓNG LỆNH THẬT THẤT BẠI: {e}")

        avg = pos.avg_entry
        pnl_pct = (price - avg) / avg * 100 if pos.direction == "LONG" else (avg - price) / avg * 100
        logger.info(f"[{KEO_PIVOT_NAME}] {symbol} ĐÓNG TẤT CẢ {pos.direction} ({len(pos.entries)} lệnh) "
                    f"| avg={avg:.4f} exit={price:.4f} PnL~{pnl_pct:+.2f}% | lý do: {reason}")
        if self.daily_stats is not None:
            self.daily_stats.record_result("TP" if pnl_pct >= 0 else "SL")
        text = _build_pivot_close_message(symbol, pos, price, pnl_pct, reason, order_err)
        await _send_telegram_message(self.chat_id, text, f"PIVOT-{symbol}-CLOSE")

    async def _open_or_dca(self, symbol: str, direction: Literal["LONG", "SHORT"], price: float,
                            bar_open: int | None) -> None:
        pos = self._position.get(symbol)
        if pos is not None and len(pos.entries) > self.max_dca:
            return

        qty = 0.0
        order_err = None
        margin_per_entry = pos.margin_per_entry if pos is not None else 0.0
        if self.executor is not None:
            try:
                if pos is None:
                    margin_per_entry = await self.executor.compute_entry_margin(symbol)
                side = "BUY" if direction == "LONG" else "SELL"
                qty = await self.executor.market_order(symbol, side, margin_per_entry, price)
            except Exception as e:
                order_err = str(e)
                logger.error(f"[{KEO_PIVOT_NAME}] {symbol} ĐẶT LỆNH THẬT THẤT BẠI: {e}")

        entry = PivotEntry(price=price, bar_open=bar_open, qty=qty)
        if pos is None:
            pos = PivotPosition(direction=direction, entries=[entry], margin_per_entry=margin_per_entry)
            self._position[symbol] = pos
            kind = "MỞ LỆNH"
            if self.daily_stats is not None:
                self.daily_stats.record_open()
        else:
            pos.entries.append(entry)
            kind = f"DCA lần {len(pos.entries) - 1}"

        logger.info(f"[{KEO_PIVOT_NAME}] {symbol} {direction} {kind} tại {price:.4f} "
                    f"(tổng {len(pos.entries)} lệnh, avg={pos.avg_entry:.4f})")
        text = _build_pivot_open_message(symbol, pos, kind, order_err)
        await _send_telegram_message(self.chat_id, text, f"PIVOT-{symbol}-{direction}")

    async def on_closed_candle(self, symbol: str, candles: list[dict]) -> None:
        if symbol not in self.symbols:
            return

        c = candles[-1]

        needs_confirm = symbol in self.dca_confirm_symbols

        armed_long = self._armed_long.pop(symbol, None)
        if armed_long is not None and c["low"] >= armed_long.low:
            pos = self._position.get(symbol)
            if pos is None:
                await self._open_or_dca(symbol, "LONG", c["close"], c.get("bar_open"))
            elif pos.direction == "LONG" and len(pos.entries) <= self.max_dca and needs_confirm:
                await self._open_or_dca(symbol, "LONG", c["close"], c.get("bar_open"))

        armed_short = self._armed_short.pop(symbol, None)
        if armed_short is not None and c["high"] <= armed_short.high:
            pos = self._position.get(symbol)
            if pos is None:
                await self._open_or_dca(symbol, "SHORT", c["close"], c.get("bar_open"))
            elif pos.direction == "SHORT" and len(pos.entries) <= self.max_dca and needs_confirm:
                await self._open_or_dca(symbol, "SHORT", c["close"], c.get("bar_open"))

        ph, pl = _detect_running_extreme(candles, self.length)

        if pl is not None:
            pos = self._position.get(symbol)
            if pos is not None and pos.direction == "SHORT":
                await self._close_all(symbol, c["close"], "tín hiệu tăng (▲) xuất hiện")
                pos = None
            if pos is None:
                self._armed_long[symbol] = pl
            elif pos.direction == "LONG" and len(pos.entries) <= self.max_dca:
                if needs_confirm:
                    self._armed_long[symbol] = pl
                else:
                    await self._open_or_dca(symbol, "LONG", c["close"], c.get("bar_open"))

        if ph is not None:
            pos = self._position.get(symbol)
            if pos is not None and pos.direction == "LONG":
                await self._close_all(symbol, c["close"], "tín hiệu giảm (▼) xuất hiện")
                pos = None
            if pos is None:
                self._armed_short[symbol] = ph
            elif pos.direction == "SHORT" and len(pos.entries) <= self.max_dca:
                if needs_confirm:
                    self._armed_short[symbol] = ph
                else:
                    await self._open_or_dca(symbol, "SHORT", c["close"], c.get("bar_open"))


def _banner() -> None:
    logger.info("=" * 50)
    logger.info("  Binance Futures Song Kiem Signal Bot (WebSocket real-time)")
    logger.info("=" * 50)
    if AUTO_TOP_SYMBOLS:
        logger.info(f"  Symbol    : Tự động top {TOP_SYMBOLS_COUNT} (LONG/SHORT) | "
                    f"top {LEGACY_TOP_SYMBOLS_COUNT} (RSI H4)")
    else:
        logger.info(f"  Symbol    : Thủ công {len(SYMBOLS)} cặp")
    logger.info(f"  Timeframe : {INTERVAL_H1_DISPLAY} (kèo Rút Râu) | "
                f"{INTERVAL_H4_DISPLAY} (kèo RSI Đảo Biên) | "
                f"{INTERVAL_M5_DISPLAY} (Pivot DCA — {','.join(PIVOT_SYMBOLS_M5)}) | "
                f"{INTERVAL_M15_DISPLAY} (Pivot DCA — {','.join(PIVOT_SYMBOLS_M15)})")
    logger.info(f"  Nguồn nến : Futures WebSocket (path /market)")
    logger.info(f"  {KEO_RUTRAU_NAME:<22} -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID else 'OK'}  "
                f"TP/SL={DOJI_TP_PCT*100:.1f}%/{DOJI_SL_PCT*100:.1f}%")
    logger.info(f"  {KEO_LEGACY_NAME:<22} -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID_H1 else 'OK'}  "
                f"TP/SL={LEGACY_TP_PCT*100:.1f}%/{LEGACY_SL_PCT*100:.1f}%  "
                f"RSI({LEGACY_RSI_PERIOD}) armed={LEGACY_RSI_OVERSOLD}/{LEGACY_RSI_OVERBOUGHT} "
                f"bắn={LEGACY_RSI_LONG_CONFIRM}/{LEGACY_RSI_SHORT_CONFIRM} (intrabar)")
    logger.info(f"  {KEO_PIVOT_NAME:<22} -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID_PIVOT else 'OK'}  "
                f"length={PIVOT_LENGTH}  DCA tối đa={PIVOT_MAX_DCA} lần  coin={','.join(PIVOT_SYMBOLS)}  "
                f"(không TP/SL, đóng theo tín hiệu đối nghịch)")
    if PIVOT_LIVE_TRADING and BINANCE_API_KEY and BINANCE_API_SECRET:
        logger.warning(f"  ⚠️  ĐẶT LỆNH THẬT (MAINNET) ĐANG BẬT cho {KEO_PIVOT_NAME} — "
                        f"x{PIVOT_LEVERAGE} đòn bẩy, {PIVOT_ACCOUNT_PCT*100:.1f}% tài khoản/vị thế, "
                        f"{PIVOT_MARGIN_TYPE}")
        print("=" * 50)
        print(f"  ⚠️  CẢNH BÁO: ĐANG ĐẶT LỆNH THẬT BẰNG TIỀN THẬT (MAINNET) — x{PIVOT_LEVERAGE} đòn bẩy")
        print("=" * 50)
    elif PIVOT_LIVE_TRADING:
        logger.warning(f"  PIVOT_LIVE_TRADING=True nhưng thiếu BINANCE_API_KEY/SECRET -> "
                        f"{KEO_PIVOT_NAME} chạy CHỈ TÍN HIỆU, không đặt lệnh thật")
    logger.info(f"  BB        : period={BB_PERIOD}  std={BB_STD}")
    logger.info(f"  Cooldown  : {ALERT_COOLDOWN_MINUTES} phút (mới/đột biến)  |  "
                f"tối đa 1 LONG + 1 SHORT/ngày mỗi cặp (RSI H4)")
    logger.info("=" * 50)


async def _check_telegram_connection(chat_id: str, label: str) -> None:
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
    await _check_telegram_connection(TELEGRAM_CHAT_ID_PIVOT, "PIVOT-DCA")


async def _run_forever(label: str, feed: LiveFeed) -> None:
    while True:
        try:
            await feed.run()
        except Exception as e:
            logger.critical(f"[{label}] LiveFeed dừng bất ngờ, khởi động lại sau 10s: {e}", exc_info=True)
            await asyncio.sleep(10)


async def daily_stats_scheduler(stats_list: list[DailyStats]) -> None:
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

        await asyncio.sleep(61)


async def _periodic_time_sync(executor: "BinanceExecutor", interval_sec: int = 1800) -> None:
    while True:
        await asyncio.sleep(interval_sec)
        try:
            await executor.sync_time()
        except Exception as e:
            logger.error(f"[Pivot Executor] Đồng bộ lại giờ thất bại (giữ nguyên lệch cũ): {e}")


async def _main() -> None:
    _banner()
    await _check_telegram_connections()
    try:
        symbols        = await resolve_symbols(TOP_SYMBOLS_COUNT)
        legacy_symbols = await resolve_symbols(LEGACY_TOP_SYMBOLS_COUNT)

        feed         = LiveFeed(symbols, INTERVAL_H1, CANDLE_BUFFER, name="1h")
        feed_h4      = LiveFeed(legacy_symbols, INTERVAL_H4, CANDLE_BUFFER, name="4h-RSI")
        feed_pivot_m5  = LiveFeed(PIVOT_SYMBOLS_M5, INTERVAL_M5, PIVOT_CANDLE_BUFFER, name="5m-Pivot")
        feed_pivot_m15 = LiveFeed(PIVOT_SYMBOLS_M15, INTERVAL_M15, PIVOT_CANDLE_BUFFER, name="15m-Pivot")

        h1_stats      = DailyStats(KEO_RUTRAU_NAME, TELEGRAM_CHAT_ID)
        legacy_stats  = DailyStats(KEO_LEGACY_NAME, TELEGRAM_CHAT_ID_H1)
        pivot_stats   = DailyStats(KEO_PIVOT_NAME, TELEGRAM_CHAT_ID_PIVOT)

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
        h4_rsi_scanner = RsiExtremeScanner(
            legacy_symbols, TELEGRAM_CHAT_ID_H1, daily_stats=legacy_stats,
        )

        pivot_executor: "BinanceExecutor | None" = None
        if PIVOT_LIVE_TRADING and BINANCE_API_KEY and BINANCE_API_SECRET:
            candidate = BinanceExecutor(
                BINANCE_API_KEY, BINANCE_API_SECRET, PIVOT_LEVERAGE, PIVOT_MARGIN_TYPE,
                PIVOT_ACCOUNT_PCT, PIVOT_MAX_DCA + 1,
                fixed_margin_usdt=PIVOT_FIXED_MARGIN_USDT,
            )
            try:
                await candidate.sync_time()
                for sym in PIVOT_SYMBOLS:
                    await candidate.setup_symbol(sym)
                    logger.info(f"[Pivot Executor] {sym}: đã đặt {PIVOT_MARGIN_TYPE} x{PIVOT_LEVERAGE}")
                pivot_executor = candidate
            except Exception as e:
                logger.critical(f"[Pivot Executor] LỖI setup ({e}) -> {KEO_PIVOT_NAME} chạy CHỈ "
                                 f"TÍN HIỆU, KHÔNG đặt lệnh thật", exc_info=True)

        pivot_scanner = PivotDcaScanner(
            PIVOT_SYMBOLS, TELEGRAM_CHAT_ID_PIVOT, daily_stats=pivot_stats, executor=pivot_executor,
            dca_confirm_symbols=set(PIVOT_SYMBOLS_M5),
        )

        for sc in (long_scanner, short_scanner):
            feed.on_closed_candle(sc.on_closed_candle)
            feed.on_live_tick(sc.on_live_tick)

        feed_h4.on_closed_candle(h4_rsi_scanner.on_closed_candle)
        feed_h4.on_live_tick(h4_rsi_scanner.on_live_tick)

        feed_pivot_m5.on_closed_candle(pivot_scanner.on_closed_candle)
        feed_pivot_m15.on_closed_candle(pivot_scanner.on_closed_candle)

        stats_tasks = [daily_stats_scheduler([h1_stats, legacy_stats, pivot_stats])]
        if pivot_executor is not None:
            stats_tasks.append(_periodic_time_sync(pivot_executor))

        await asyncio.gather(
            _run_forever("LiveFeed-H1", feed),
            _run_forever("LiveFeed-H4", feed_h4),
            _run_forever("LiveFeed-M5-Pivot", feed_pivot_m5),
            _run_forever("LiveFeed-M15-Pivot", feed_pivot_m15),
            *stats_tasks,
        )
    except KeyboardInterrupt:
        logger.info("Bot dừng.")
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(_main())
