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
import websockets

# ═══════════════════════════════════════════════
#  CẤU HÌNH — chỉnh ở đây
# ═══════════════════════════════════════════════
TELEGRAM_TOKEN   = "8641278115:AAEB08VXrX5YJl_2zzM_SFF4JRdEwIfAj-s"   # Token bot Telegram
TELEGRAM_CHAT_ID = "-1004448248877"   # Chat ID nhận tín hiệu

AUTO_TOP_SYMBOLS  = True   # True = tự động lấy top coin theo khối lượng
TOP_SYMBOLS_COUNT = 50     # Số lượng coin theo dõi

SYMBOLS = [                # Dùng khi AUTO_TOP_SYMBOLS = False
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

INTERVAL         = "15m"
INTERVAL_DISPLAY = "M15"
CANDLE_BUFFER    = 150

BB_PERIOD = 20
BB_STD    = 2.0

SONG_KIEM_RATIO        = 0.90  # Thân nến 2 >= 90% thân nến 1
ALERT_COOLDOWN_MINUTES = 30    # Cooldown giữa 2 tín hiệu cùng coin/chiều

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


def detect_signal(symbol: str, candles: list[dict]) -> Signal | None:
    if len(candles) < BB_PERIOD + 5:
        return None

    # BB tại nến 1 — khớp bb_upper[1] trong Pine Script
    ind_prev = compute_indicators(candles[:-1])
    # BB tại nến 2 — khớp bb_upper trong Pine Script
    ind_curr = compute_indicators(candles)

    if ind_curr["bb_upper"] == 0.0:
        return None

    prev = candles[-2]   # Nến 1 (BB-touch)
    curr = candles[-1]   # Nến 2 (xác nhận)

    prev_body = abs(prev["close"] - prev["open"])
    curr_body = abs(curr["close"] - curr["open"])
    if prev_body == 0:
        return None

    prev_bullish = prev["close"] > prev["open"]
    curr_bullish = curr["close"] > curr["open"]

    # ── SHORT ─────────────────────────────────
    short_bb      = prev_bullish and prev["close"] >= ind_prev["bb_upper"]
    short_pattern = not curr_bullish and curr_body >= SONG_KIEM_RATIO * prev_body
    short_vol     = curr["volume"] > prev["volume"]

    if short_bb and short_pattern and short_vol:
        sl = max(prev["high"], curr["high"])
        logger.info(f"{symbol} SHORT | Entry={curr['close']:.4f}  SL={sl:.4f}")
        return Signal(symbol=symbol, direction="SHORT", price=curr["close"], sl=sl, ind=ind_curr)

    if short_bb and short_pattern:
        logger.debug(f"{symbol} SHORT gần đủ: BB+Pattern OK, Vol chưa đủ")

    # ── LONG ──────────────────────────────────
    long_bb      = not prev_bullish and prev["close"] <= ind_prev["bb_lower"]
    long_pattern = curr_bullish and curr_body >= SONG_KIEM_RATIO * prev_body
    long_vol     = curr["volume"] > prev["volume"]

    if long_bb and long_pattern and long_vol:
        sl = min(prev["low"], curr["low"])
        logger.info(f"{symbol} LONG  | Entry={curr['close']:.4f}  SL={sl:.4f}")
        return Signal(symbol=symbol, direction="LONG", price=curr["close"], sl=sl, ind=ind_curr)

    if long_bb and long_pattern:
        logger.debug(f"{symbol} LONG gần đủ: BB+Pattern OK, Vol chưa đủ")

    return None

# ═══════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════
def _fmt(price: float) -> str:
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


def _build_message(signal: Signal) -> str:
    if signal.direction == "SHORT":
        header    = "🔴 SHORT SIGNAL"
        bb_label  = "Close nến xanh chạm Bollinger Upper"
        vol_label = "Vol nến đỏ > Vol nến xanh"
    else:
        header    = "🟢 LONG SIGNAL"
        bb_label  = "Close nến đỏ chạm Bollinger Lower"
        vol_label = "Vol nến xanh > Vol nến đỏ"

    return (
        f"*{header}*\n\n"
        f"Coin: `{signal.symbol}`\n"
        f"Timeframe: {INTERVAL_DISPLAY}\n\n"
        f"Điều kiện:\n"
        f"✓ {bb_label}\n"
        f"✓ Xuất hiện cặp nến Song Kiếm\n"
        f"✓ {vol_label}\n\n"
        f"Entry: `{_fmt(signal.price)}`\n"
        f"SL: `{_fmt(signal.sl)}`"
    )


async def send_signal(signal: Signal) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Chưa cấu hình TELEGRAM_TOKEN / TELEGRAM_CHAT_ID")
        return

    text    = _build_message(signal)
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}

    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    logger.info(f"[TG] Gửi: {signal.symbol} {signal.direction}")
                else:
                    body = await resp.text()
                    logger.error(f"[TG] Lỗi {resp.status}: {body}")
    except Exception as e:
        logger.error(f"[TG] Không gửi được: {e}")

# ═══════════════════════════════════════════════
#  BINANCE CLIENT
# ═══════════════════════════════════════════════
_FUTURES_REST   = "https://fapi.binance.com"
_FUTURES_WS     = "wss://fstream.binance.com/stream"
_RECONNECT_DELAY = 5


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
        logger.info(f"Top {n} cặp: {', '.join(symbols)}")
        return symbols
    except Exception as e:
        logger.error(f"Không lấy được top symbol: {e}")
        return []


class BinanceClient:
    def __init__(self, symbols: list[str], interval: str, buffer_size: int):
        self.symbols      = [s.upper() for s in symbols]
        self.interval     = interval
        self.buffer_size  = buffer_size
        self.candles: dict[str, deque] = defaultdict(lambda: deque(maxlen=buffer_size))
        self._queue: asyncio.Queue     = asyncio.Queue()
        self._reconnect   = 0

    async def _fetch_initial(self) -> None:
        logger.info(f"Nạp lịch sử {len(self.symbols)} coin...")
        ok, fail = 0, []
        async with aiohttp.ClientSession() as session:
            for sym in self.symbols:
                try:
                    params = {"symbol": sym, "interval": self.interval, "limit": self.buffer_size}
                    async with session.get(
                        f"{_FUTURES_REST}/fapi/v1/klines", params=params,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status != 200:
                            raise ValueError(f"HTTP {resp.status}")
                        for k in (await resp.json())[:-1]:  # bỏ nến chưa đóng
                            self.candles[sym].append({
                                "open": float(k[1]), "high": float(k[2]),
                                "low":  float(k[3]), "close": float(k[4]),
                                "volume": float(k[5]),
                            })
                    logger.info(f"  ✓ {sym}: {len(self.candles[sym])} nến")
                    ok += 1
                except Exception as e:
                    logger.error(f"  ✗ {sym}: {e}")
                    fail.append(sym)
        logger.info(f"Nạp xong {ok}/{len(self.symbols)}" +
                    (f" | Lỗi: {', '.join(fail)}" if fail else ""))

    def _on_kline(self, data: dict) -> None:
        k = data["k"]
        if not k["x"]:   # chỉ xử lý nến đã đóng
            return
        sym = k["s"]
        self.candles[sym].append({
            "open": float(k["o"]), "high": float(k["h"]),
            "low":  float(k["l"]), "close": float(k["c"]),
            "volume": float(k["v"]),
        })
        self._candle_count = getattr(self, "_candle_count", 0) + 1
        if self._candle_count % 10 == 0:
            logger.info(f"Nến đóng #{self._candle_count}: {sym} | {float(k['c']):.4f}")
        if len(self.candles[sym]) >= 55:
            self._queue.put_nowait((sym, list(self.candles[sym])))

    async def _ws_loop(self) -> None:
        streams = "/".join(f"{s.lower()}@kline_{self.interval}" for s in self.symbols)
        url     = f"{_FUTURES_WS}?streams={streams}"
        while True:
            try:
                logger.info("Kết nối WebSocket Binance Futures...")
                async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
                    self._reconnect = 0
                    logger.info(f"WebSocket OK — {len(self.symbols)} stream")
                    async for raw in ws:
                        msg = json.loads(raw)
                        if "data" in msg:
                            self._on_kline(msg["data"])
            except websockets.ConnectionClosed as e:
                self._reconnect += 1
                logger.warning(f"WS đóng (#{self._reconnect}): {e} — retry {_RECONNECT_DELAY}s")
            except Exception as e:
                self._reconnect += 1
                logger.error(f"WS lỗi (#{self._reconnect}): {e} — retry {_RECONNECT_DELAY}s")
            await asyncio.sleep(_RECONNECT_DELAY)

    async def _process_queue(self, cb: Callable[[str, list], Awaitable[None]]) -> None:
        while True:
            sym, candles = await self._queue.get()
            try:
                await cb(sym, candles)
            except Exception as e:
                logger.error(f"Lỗi xử lý {sym}: {e}", exc_info=True)

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(900)  # 15 phút
            total = sum(len(v) for v in self.candles.values())
            logger.info(f"[HEARTBEAT] Bot đang chạy — {len(self.symbols)} cặp | tổng {total} nến trong buffer")

    async def run(self, cb: Callable[[str, list], Awaitable[None]]) -> None:
        await self._fetch_initial()
        await asyncio.gather(self._ws_loop(), self._process_queue(cb), self._heartbeat())

# ═══════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════
class Scanner:
    def __init__(self) -> None:
        self._last_alert: dict[str, datetime] = {}

    async def _build_client(self) -> BinanceClient:
        if AUTO_TOP_SYMBOLS:
            logger.info(f"Lấy top {TOP_SYMBOLS_COUNT} cặp từ Binance...")
            symbols = await fetch_top_symbols(TOP_SYMBOLS_COUNT)
            if not symbols:
                logger.warning("Không lấy được, dùng danh sách cố định")
                symbols = SYMBOLS
        else:
            symbols = SYMBOLS
            logger.info(f"Dùng {len(symbols)} cặp từ cấu hình")
        return BinanceClient(symbols, INTERVAL, CANDLE_BUFFER)

    def _cooldown_left(self, symbol: str, direction: str) -> int:
        last = self._last_alert.get(f"{symbol}_{direction}")
        if last is None:
            return 0
        remaining = timedelta(minutes=ALERT_COOLDOWN_MINUTES) - (datetime.now() - last)
        return max(0, int(remaining.total_seconds()))

    async def on_candle(self, symbol: str, candles: list[dict]) -> None:
        signal = detect_signal(symbol, candles)
        if signal is None:
            return

        left = self._cooldown_left(symbol, signal.direction)
        if left > 0:
            m, s = divmod(left, 60)
            logger.info(f"{symbol} {signal.direction}: cooldown còn {m}p{s:02d}s")
            return

        self._last_alert[f"{symbol}_{signal.direction}"] = datetime.now()
        logger.info(f">>> TÍN HIỆU: {symbol} {signal.direction} | Entry={signal.price} | SL={signal.sl}")
        await send_signal(signal)

    async def run(self) -> None:
        client = await self._build_client()
        logger.info(f"Sẵn sàng — theo dõi {len(client.symbols)} cặp")
        await client.run(self.on_candle)

# ═══════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════
def _banner() -> None:
    logger.info("=" * 50)
    logger.info("  Binance Futures Song Kiem Signal Bot")
    logger.info("=" * 50)
    if AUTO_TOP_SYMBOLS:
        logger.info(f"  Symbol    : Tự động top {TOP_SYMBOLS_COUNT}")
    else:
        logger.info(f"  Symbol    : Thủ công {len(SYMBOLS)} cặp")
    logger.info(f"  Timeframe : {INTERVAL_DISPLAY}")
    logger.info(f"  BB        : period={BB_PERIOD}  std={BB_STD}")
    logger.info(f"  SK ratio  : {SONG_KIEM_RATIO}")
    logger.info(f"  Cooldown  : {ALERT_COOLDOWN_MINUTES} phút")
    logger.info("=" * 50)


async def _send_startup_message() -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("=" * 50)
        print("  [LỖI] Chưa điền TELEGRAM_TOKEN hoặc TELEGRAM_CHAT_ID")
        print("=" * 50)
        return
    text = (
        "✅ *Bot đã khởi động*\n\n"
        f"Timeframe: `{INTERVAL_DISPLAY}`\n"
        f"Theo dõi: `{'Top ' + str(TOP_SYMBOLS_COUNT) + ' coin' if AUTO_TOP_SYMBOLS else str(len(SYMBOLS)) + ' coin'}`\n"
        f"Cooldown: `{ALERT_COOLDOWN_MINUTES} phút`\n\n"
        "Đang chờ tín hiệu Song Kiếm..."
    )
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    print("  Đang kiểm tra kết nối Telegram...")
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    print("=" * 50)
                    print("  [TELEGRAM] Kết nối thành công ✓")
                    print("  Tin nhắn khởi động đã gửi vào Telegram")
                    print("=" * 50)
                    logger.info("[TG] Gửi tin khởi động thành công")
                else:
                    body = await resp.text()
                    print("=" * 50)
                    print(f"  [LỖI TELEGRAM] HTTP {resp.status}")
                    print(f"  Chi tiết: {body}")
                    print("  Kiểm tra lại TELEGRAM_TOKEN và TELEGRAM_CHAT_ID")
                    print("=" * 50)
                    logger.error(f"[TG] Lỗi {resp.status}: {body}")
    except Exception as e:
        print("=" * 50)
        print(f"  [LỖI TELEGRAM] Không kết nối được: {e}")
        print("  Kiểm tra lại token và chat ID")
        print("=" * 50)
        logger.error(f"[TG] Không gửi được: {e}")


async def _main() -> None:
    _banner()
    await _send_startup_message()
    try:
        await Scanner().run()
    except KeyboardInterrupt:
        logger.info("Bot dừng.")
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(_main())
