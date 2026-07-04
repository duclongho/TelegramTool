#!/usr/bin/env python3
"""
Binance Futures Song Kiem Signal Bot
Chỉnh TELEGRAM_TOKEN, TELEGRAM_CHAT_ID trước khi chạy.
"""
import asyncio
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Literal, TypedDict

import aiohttp

# ═══════════════════════════════════════════════
#  CẤU HÌNH — chỉnh ở đây
# ═══════════════════════════════════════════════
TELEGRAM_TOKEN   = "8641278115:AAEB08VXrX5YJl_2zzM_SFF4JRdEwIfAj-s"   # Token bot Telegram
TELEGRAM_CHAT_ID = "-1004448248877"   # Chat ID nhận tín hiệu

AUTO_TOP_SYMBOLS  = True   # True = tự động lấy top coin theo khối lượng
TOP_SYMBOLS_COUNT = 100     # Số lượng coin theo dõi

SYMBOLS = [                # Dùng khi AUTO_TOP_SYMBOLS = False
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

INTERVAL         = "4h"
INTERVAL_DISPLAY = "H4"
CANDLE_BUFFER    = 150

BB_PERIOD = 20
BB_STD    = 2.0

VOL_RATIO_MIN          = 0.95  # Vol nến 2 (sau) tối thiểu 95% vol nến 1 (trước)
VOL_RATIO_MAX          = 1.15  # Vol nến 2 (sau) tối đa 115% vol nến 1 (trước)
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
    direction: Literal["LONG"]
    price:     float
    sl:        float
    ind:       Indicators


def detect_signal(symbol: str, candles: list[dict]) -> Signal | None:
    if len(candles) < BB_PERIOD + 5:
        return None

    ind = compute_indicators(candles)
    if ind["bb_middle"] == 0.0:
        return None

    prev = candles[-2]   # Nến 1
    curr = candles[-1]   # Nến 2

    # Cả hai nến phải là nến tăng
    if not (prev["close"] > prev["open"] and curr["close"] > curr["open"]):
        return None

    # Cả hai mở cửa dưới BB giữa
    if not (prev["open"] < ind["bb_middle"] and curr["open"] < ind["bb_middle"]):
        return None

    # Vol nến sau phải bằng 95% đến 115% vol nến trước
    vol_ratio = curr["volume"] / prev["volume"]
    if not (VOL_RATIO_MIN <= vol_ratio <= VOL_RATIO_MAX):
        return None

    sl = min(prev["low"], curr["low"])
    logger.info(f"{symbol} LONG | Entry={curr['close']:.4f}  SL={sl:.4f}  VolRatio={vol_ratio:.2f}")
    return Signal(symbol=symbol, direction="LONG", price=curr["close"], sl=sl, ind=ind)

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
    return (
        f"*🟢 LONG SIGNAL*\n\n"
        f"Coin: `{signal.symbol}`\n"
        f"Timeframe: {INTERVAL_DISPLAY}\n\n"
        f"Điều kiện:\n"
        f"✓ Hai nến tăng liên tiếp\n"
        f"✓ Cả hai nến đều ở BB dưới đến BB giữa\n"
        f"✓ Khối lượng hai nến gần bằng nhau\n\n"
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
_FUTURES_REST = "https://fapi.binance.com"


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
        self._last_close: dict[str, int] = {}   # symbol -> close_time_ms đã xử lý
        self._interval_ms = self._parse_interval_ms(interval)

    @staticmethod
    def _parse_interval_ms(interval: str) -> int:
        units = {"m": 60, "h": 3600, "d": 86400}
        return int(interval[:-1]) * units[interval[-1]] * 1000

    async def _fetch_next_close_ms(self) -> int:
        """Lấy closeTime của nến đang mở từ Binance (chính xác nhất)."""
        sym = self.symbols[0]
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                params = {"symbol": sym, "interval": self.interval, "limit": 1}
                async with session.get(
                    f"{_FUTURES_REST}/fapi/v1/klines", params=params,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    rows = await resp.json()
            return int(rows[0][6])  # closeTime của nến đang mở
        except Exception as e:
            logger.error(f"Không lấy được closeTime từ Binance: {e} — dùng tính toán local")
            now_ms = int(datetime.now().timestamp() * 1000)
            return ((now_ms // self._interval_ms) + 1) * self._interval_ms - 1

    async def _fetch_initial(self) -> None:
        logger.info(f"Nạp lịch sử {len(self.symbols)} coin...")
        ok, fail = 0, []
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            for sym in self.symbols:
                try:
                    params = {"symbol": sym, "interval": self.interval, "limit": self.buffer_size}
                    async with session.get(
                        f"{_FUTURES_REST}/fapi/v1/klines", params=params,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        if resp.status != 200:
                            raise ValueError(f"HTTP {resp.status}")
                        rows = await resp.json()
                        for k in rows[:-1]:   # bỏ nến đang mở
                            self.candles[sym].append({
                                "open": float(k[1]), "high": float(k[2]),
                                "low":  float(k[3]), "close": float(k[4]),
                                "volume": float(k[5]),
                            })
                        # Ghi nhớ close_time của nến cuối để tránh xử lý lại
                        if rows:
                            self._last_close[sym] = int(rows[-2][6])
                    logger.info(f"  ✓ {sym}: {len(self.candles[sym])} nến")
                    ok += 1
                except Exception as e:
                    logger.error(f"  ✗ {sym}: {e}")
                    fail.append(sym)
        logger.info(f"Nạp xong {ok}/{len(self.symbols)}" +
                    (f" | Lỗi: {', '.join(fail)}" if fail else ""))

    async def _poll_all(self, cb: Callable[[str, list], Awaitable[None]]) -> None:
        """Fetch 2 nến gần nhất của mỗi symbol, xử lý nến vừa đóng nếu mới."""
        closed = 0
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            for sym in self.symbols:
                try:
                    params = {"symbol": sym, "interval": self.interval, "limit": 2}
                    async with session.get(
                        f"{_FUTURES_REST}/fapi/v1/klines", params=params,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status != 200:
                            continue
                        rows = await resp.json()
                    if len(rows) < 2:
                        continue
                    k          = rows[0]               # nến đã đóng
                    close_time = int(k[6])
                    if self._last_close.get(sym) == close_time:
                        continue                        # đã xử lý rồi
                    self._last_close[sym] = close_time
                    self.candles[sym].append({
                        "open": float(k[1]), "high": float(k[2]),
                        "low":  float(k[3]), "close": float(k[4]),
                        "volume": float(k[5]),
                    })
                    closed += 1
                    if len(self.candles[sym]) >= 55:
                        await cb(sym, list(self.candles[sym]))
                except Exception as e:
                    logger.error(f"Poll lỗi {sym}: {e}")
        if closed:
            logger.info(f"Xử lý {closed} nến đóng mới")

    async def run(self, cb: Callable[[str, list], Awaitable[None]]) -> None:
        await self._fetch_initial()
        cycle = 0
        while True:
            next_close_ms = await self._fetch_next_close_ms()
            now_ms = int(datetime.now().timestamp() * 1000)
            wait   = max(1.0, (next_close_ms - now_ms + 8000) / 1000)
            close_dt = datetime.utcfromtimestamp(next_close_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Chờ {wait:.0f}s — nến H4 đóng lúc {close_dt} UTC")
            await asyncio.sleep(wait)
            cycle += 1
            logger.info(f"[Chu kỳ #{cycle}] Đang kiểm tra nến mới...")
            await self._poll_all(cb)

            # Retry sau 10s cho các coin bị miss
            missed = [s for s in self.symbols if s not in self._last_close
                      or self._last_close[s] < self._expected_close_ms()]
            if missed:
                logger.info(f"Retry {len(missed)} coin bị miss sau 10s...")
                await asyncio.sleep(10)
                await self._poll_all(cb)

    def _expected_close_ms(self) -> int:
        """Close time ms của nến vừa đóng."""
        now_ms = int(datetime.now().timestamp() * 1000)
        return (now_ms // self._interval_ms) * self._interval_ms - 1

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
    logger.info(f"  Vol ratio : {VOL_RATIO_MIN} - {VOL_RATIO_MAX}")
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
        "Đang chờ tín hiệu LONG..."
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
