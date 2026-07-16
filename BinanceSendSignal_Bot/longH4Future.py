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
TELEGRAM_TOKEN      = "8641278115:AAEB08VXrX5YJl_2zzM_SFF4JRdEwIfAj-s"   # Token bot Telegram
TELEGRAM_CHAT_ID    = "-1004448248877"   # Chat ID nhận LONG SIGNAL (H1) + SHORT SIGNAL (H1) — điều kiện mới, chạm BB
TELEGRAM_CHAT_ID_H1 = "-1004340326145"   # Chat ID nhận tín hiệu SHORT (H1) cũ — giữ nguyên điều kiện + TP/SL cũ

AUTO_TOP_SYMBOLS  = True   # True = tự động lấy top coin theo khối lượng
TOP_SYMBOLS_COUNT = 100     # Số lượng coin theo dõi

SYMBOLS = [                # Dùng khi AUTO_TOP_SYMBOLS = False
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

CANDLE_BUFFER = 150

INTERVAL_H1         = "1h"   # Timeframe dùng chung cho tất cả các kèo
INTERVAL_H1_DISPLAY = "H1"
H1_POLL_STAGGER_SEC = 30     # Trễ thêm N giây khi poll các scanner phụ để tránh trùng đợt poll

BB_PERIOD = 20
BB_STD    = 2.0

BAND_TOUCH_TOLERANCE = 0.05   # 5% - coi là "bám sát" đường BB giữa (kèo Short H1 cũ)
VOL_RATIO_MIN        = 0.95   # Vol nến 2 (sau) tối thiểu 95% vol nến 1 (trước) (kèo Short H1 cũ)
VOL_RATIO_MAX        = 1.15   # Vol nến 2 (sau) tối đa 115% vol nến 1 (trước) (kèo Short H1 cũ)

MIN_CANDLES_FOR_SIGNAL = BB_PERIOD + 5  # Số nến tối thiểu cần có trước khi bắt đầu xét tín hiệu

ALERT_COOLDOWN_MINUTES = 30    # Cooldown giữa 2 tín hiệu cùng coin/chiều

NEW_TP_PCT = 0.025   # Chốt lời cố định 2.5% (kèo Long/Short H1 mới — điều kiện chạm BB)
NEW_SL_PCT = 0.01    # Cắt lỗ cố định 1% (kèo Long/Short H1 mới)

LEGACY_TP_PCT = 0.04   # Chốt lời cố định 4% (kèo Short H1 cũ)
LEGACY_SL_PCT = 0.02   # Cắt lỗ cố định 2% (kèo Short H1 cũ)

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


def _near_band(price: float, band: float, tolerance: float = BAND_TOUCH_TOLERANCE) -> bool:
    """True nếu giá nằm sát đường band (trong khoảng dung sai %)."""
    return band > 0 and abs(price - band) / band <= tolerance

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


def detect_signal(symbol: str, candles: list[dict],
                   direction: Literal["LONG", "SHORT"] = "LONG") -> Signal | None:
    if len(candles) < BB_PERIOD + 3:
        return None

    before = candles[-3]   # Nến ngay trước nến #1 — dùng để xác nhận nến #1 là nến ĐẦU TIÊN của chuỗi màu
    prev   = candles[-2]   # Nến màu thứ 1 — nến chạm/xuyên band
    curr   = candles[-1]   # Nến màu thứ 2 — xác nhận, báo tín hiệu khi đóng cửa

    # BB tính đến trước nến màu thứ 1, tránh self-reference
    ind = compute_indicators(candles[:-2])
    if ind["bb_middle"] == 0.0:
        return None

    if direction == "LONG":
        # Nến 1: xanh, chạm/xuyên BB dưới, và là nến xanh đầu tiên (nến trước đó không xanh)
        if not (prev["close"] > prev["open"] and prev["low"] <= ind["bb_lower"]):
            return None
        if before["close"] > before["open"]:
            return None   # Nến trước cũng xanh -> đây là chuỗi tiếp diễn, đã báo ở nến trước rồi
        if not (curr["close"] > curr["open"]):
            return None
        # Cả 2 nến đều không được có râu vượt qua BB giữa (nằm hẳn dưới band giữa)
        if prev["high"] >= ind["bb_middle"] or curr["high"] >= ind["bb_middle"]:
            return None
    else:
        # Nến 1: đỏ, chạm/xuyên BB trên, và là nến đỏ đầu tiên (nến trước đó không đỏ)
        if not (prev["close"] < prev["open"] and prev["high"] >= ind["bb_upper"]):
            return None
        if before["close"] < before["open"]:
            return None   # Nến trước cũng đỏ -> chuỗi tiếp diễn, đã báo ở nến trước rồi
        if not (curr["close"] < curr["open"]):
            return None
        # Cả 2 nến đều không được có râu vượt qua BB giữa (nằm hẳn trên band giữa)
        if prev["low"] <= ind["bb_middle"] or curr["low"] <= ind["bb_middle"]:
            return None

    logger.info(f"{symbol} {direction} | Entry={curr['close']:.4f}")
    return Signal(symbol=symbol, direction=direction, price=curr["close"], sl=0.0, ind=ind)


def detect_legacy_signal(symbol: str, candles: list[dict]) -> Signal | None:
    """Kèo Short H1 cũ — điều kiện gốc (2 nến tăng bám/vượt biên giữa BB + vol ratio),
    chỉ đổi nhãn hiển thị thành SHORT, giữ nguyên logic phát hiện."""
    if len(candles) < BB_PERIOD + 5:
        return None

    # BB tính đến trước khi nến 2 đóng, tránh self-reference
    ind = compute_indicators(candles[:-1])
    if ind["bb_middle"] == 0.0:
        return None

    prev = candles[-2]   # Nến 1
    curr = candles[-1]   # Nến 2

    # Cả hai nến phải là nến tăng
    if not (prev["close"] > prev["open"] and curr["close"] > curr["open"]):
        return None

    # Nến 1: giá tăng bám biên giữa
    if not _near_band(prev["close"], ind["bb_middle"]):
        return None

    # Nến 2: vượt biên giữa (đóng cửa hẳn trên biên giữa)
    if not (curr["open"] < ind["bb_middle"] < curr["close"]):
        return None

    # Vol nến sau phải bằng 95% đến 115% vol nến trước
    if prev["volume"] <= 0:
        return None
    vol_ratio = curr["volume"] / prev["volume"]
    if not (VOL_RATIO_MIN <= vol_ratio <= VOL_RATIO_MAX):
        return None

    logger.info(f"{symbol} SHORT (legacy) | Entry={curr['close']:.4f}  VolRatio={vol_ratio:.2f}")
    return Signal(symbol=symbol, direction="SHORT", price=curr["close"], sl=0.0, ind=ind)

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
    is_short = signal.direction == "SHORT"
    emoji    = "🔴" if is_short else "🟢"
    band     = "trên" if is_short else "dưới"
    color    = "đỏ" if is_short else "xanh"
    return (
        f"*{emoji} {signal.direction} SIGNAL ({interval_display})*\n\n"
        f"Coin: `{signal.symbol}`\n\n"
        f"Điều kiện:\n"
        f"✓ Nến {color} #1 chạm/xuyên BB {band}, là nến {color} đầu tiên\n"
        f"✓ Nến {color} #2 đóng cửa xác nhận\n"
        f"✓ Cả 2 nến không có râu vượt qua BB giữa\n\n"
        f"Entry: `{_fmt(signal.price)}`\n"
        f"TP: `{_fmt(tp)}`\n"
        f"SL: `{_fmt(signal.sl)}`"
    )


def _build_legacy_message(signal: Signal, interval_display: str, tp: float) -> str:
    return (
        f"*🔴 {signal.direction} SIGNAL ({interval_display})*\n\n"
        f"Coin: `{signal.symbol}`\n\n"
        f"Điều kiện:\n"
        f"✓ Hai nến tăng liên tiếp\n"
        f"✓ Nến 1 bám biên giữa, nến 2 vượt biên giữa\n"
        f"✓ Vol nến 2 bằng {VOL_RATIO_MIN*100:.0f}%-{VOL_RATIO_MAX*100:.0f}% vol nến 1\n\n"
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
    def __init__(self, symbols: list[str], interval: str, buffer_size: int,
                 interval_display: str | None = None, poll_offset_sec: float = 0.0):
        self.symbols          = [s.upper() for s in symbols]
        self.interval         = interval
        self.interval_display = interval_display or interval
        self.buffer_size      = buffer_size
        self.poll_offset_sec  = poll_offset_sec   # Trễ thêm để tránh trùng giờ poll với client khác
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
        logger.info(f"[{self.interval_display}] Nạp lịch sử {len(self.symbols)} coin...")
        ok, fail = 0, []
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            for sym in self.symbols:
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
                            # Ghi nhớ close_time của nến cuối để tránh xử lý lại
                            if rows:
                                self._last_close[sym] = int(rows[-2][6])
                        logger.info(f"  ✓ {sym}: {len(self.candles[sym])} nến")
                        ok += 1
                        break
                    except Exception as e:
                        if attempt == 2:
                            logger.error(f"  ✗ {sym}: {e}")
                            fail.append(sym)
                        else:
                            await asyncio.sleep(1)
        logger.info(f"[{self.interval_display}] Nạp xong {ok}/{len(self.symbols)}" +
                    (f" | Lỗi: {', '.join(fail)}" if fail else ""))

    async def _poll_all(self, cb: Callable[[str, list], Awaitable[None]],
                         symbols: list[str] | None = None) -> None:
        """Fetch 2 nến gần nhất của các symbol chỉ định (mặc định: tất cả), xử lý nến vừa đóng nếu mới."""
        targets = symbols if symbols is not None else self.symbols
        closed = 0
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            for sym in targets:
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
                    if len(self.candles[sym]) >= MIN_CANDLES_FOR_SIGNAL:
                        await cb(sym, list(self.candles[sym]))
                except Exception as e:
                    logger.error(f"Poll lỗi {sym}: {e}")
        if closed:
            logger.info(f"[{self.interval_display}] Xử lý {closed} nến đóng mới")

    async def run(self, cb: Callable[[str, list], Awaitable[None]]) -> None:
        await self._fetch_initial()
        cycle = 0
        while True:
            next_close_ms = await self._fetch_next_close_ms()
            now_ms = int(datetime.now().timestamp() * 1000)
            wait   = max(1.0, (next_close_ms - now_ms + 8000) / 1000) + self.poll_offset_sec
            close_dt = datetime.utcfromtimestamp(next_close_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[{self.interval_display}] Chờ {wait:.0f}s — nến đóng lúc {close_dt} UTC")
            await asyncio.sleep(wait)
            cycle += 1
            logger.info(f"[{self.interval_display}][Chu kỳ #{cycle}] Đang kiểm tra nến mới...")
            await self._poll_all(cb)

            # Retry sau 10s — chỉ cho các coin bị miss, không quét lại toàn bộ
            missed = [s for s in self.symbols if s not in self._last_close
                      or self._last_close[s] < self._expected_close_ms()]
            if missed:
                logger.info(f"[{self.interval_display}] Retry {len(missed)} coin bị miss sau 10s...")
                await asyncio.sleep(10)
                await self._poll_all(cb, symbols=missed)

    def _expected_close_ms(self) -> int:
        """Close time ms của nến vừa đóng."""
        now_ms = int(datetime.now().timestamp() * 1000)
        return (now_ms // self._interval_ms) * self._interval_ms - 1

# ═══════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════
async def resolve_symbols() -> list[str]:
    if AUTO_TOP_SYMBOLS:
        logger.info(f"Lấy top {TOP_SYMBOLS_COUNT} cặp từ Binance...")
        symbols = await fetch_top_symbols(TOP_SYMBOLS_COUNT)
        if not symbols:
            logger.warning("Không lấy được, dùng danh sách cố định")
            symbols = list(SYMBOLS)
    else:
        symbols = list(SYMBOLS)
        logger.info(f"Dùng {len(symbols)} cặp từ cấu hình")
    return symbols


class Scanner:
    def __init__(self, symbols: list[str], interval: str, interval_display: str,
                 chat_id: str, detect_fn: Callable[[str, list[dict]], Signal | None],
                 tp_pct: float, sl_pct: float,
                 message_builder: Callable[[Signal, str, float], str] = _build_message,
                 poll_offset_sec: float = 0.0) -> None:
        self.symbols          = symbols
        self.interval         = interval
        self.interval_display = interval_display
        self.chat_id          = chat_id
        self.detect_fn        = detect_fn
        self.tp_pct           = tp_pct
        self.sl_pct           = sl_pct
        self.message_builder  = message_builder
        self.poll_offset_sec  = poll_offset_sec
        self._last_alert: dict[str, datetime] = {}
        self._positions: dict[str, Position] = {}   # symbol -> lệnh đang mở

    def _cooldown_left(self, symbol: str, direction: str) -> int:
        last = self._last_alert.get(f"{symbol}_{direction}")
        if last is None:
            return 0
        remaining = timedelta(minutes=ALERT_COOLDOWN_MINUTES) - (datetime.now() - last)
        return max(0, int(remaining.total_seconds()))

    async def _check_position(self, symbol: str, candle: dict) -> None:
        """Sau mỗi nến mới, kiểm tra lệnh đang mở của coin này đã chạm TP hay SL chưa."""
        pos = self._positions.get(symbol)
        if pos is None:
            return

        if pos.direction == "SHORT":
            hit_tp = candle["low"]  <= pos.tp
            hit_sl = candle["high"] >= pos.sl
        else:
            hit_tp = candle["high"] >= pos.tp
            hit_sl = candle["low"]  <= pos.sl

        if not (hit_tp or hit_sl):
            return

        if hit_tp and hit_sl:
            # Cả 2 mốc bị chạm trong cùng 1 nến — ước lượng theo hướng nến để chọn mốc chạm trước
            bearish = candle["close"] <= candle["open"]
            hit: Literal["TP", "SL"] = ("TP" if bearish else "SL") if pos.direction == "SHORT" \
                else ("SL" if bearish else "TP")
        else:
            hit = "TP" if hit_tp else "SL"

        logger.info(f"[{self.interval_display}] {symbol} {pos.direction} {hit} | "
                    f"Entry={pos.entry:.4f}  {hit}={(pos.tp if hit == 'TP' else pos.sl):.4f}")
        await send_close_alert(pos, self.chat_id, self.interval_display, hit)
        del self._positions[symbol]

    async def on_candle(self, symbol: str, candles: list[dict]) -> None:
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

    async def run(self) -> None:
        client = BinanceClient(
            self.symbols, self.interval, CANDLE_BUFFER,
            interval_display=self.interval_display, poll_offset_sec=self.poll_offset_sec,
        )
        logger.info(f"[{self.interval_display}] Sẵn sàng — theo dõi {len(client.symbols)} cặp")
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
    logger.info(f"  Timeframe : {INTERVAL_H1_DISPLAY}  (chạy cho cả 3 kèo)")
    logger.info(f"  LONG/SHORT mới (chạm BB) -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID else 'OK'}  "
                f"TP/SL={NEW_TP_PCT*100:.1f}%/{NEW_SL_PCT*100:.1f}%")
    logger.info(f"  SHORT cũ (bám biên giữa)  -> chat_id={'CHƯA CẤU HÌNH' if not TELEGRAM_CHAT_ID_H1 else 'OK'}  "
                f"TP/SL={LEGACY_TP_PCT*100:.1f}%/{LEGACY_SL_PCT*100:.1f}%")
    logger.info(f"  BB        : period={BB_PERIOD}  std={BB_STD}")
    logger.info(f"  Cooldown  : {ALERT_COOLDOWN_MINUTES} phút")
    logger.info("=" * 50)


async def _send_startup_message_to(chat_id: str, label: str, text: str) -> None:
    if not TELEGRAM_TOKEN or not chat_id:
        print("=" * 50)
        print(f"  [LỖI] Chưa điền TELEGRAM_TOKEN hoặc chat ID cho {label}")
        print("=" * 50)
        return
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    print(f"  Đang kiểm tra kết nối Telegram ({label})...")
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    print("=" * 50)
                    print(f"  [TELEGRAM] Kết nối thành công ✓ ({label})")
                    print("  Tin nhắn khởi động đã gửi vào Telegram")
                    print("=" * 50)
                    logger.info(f"[TG-{label}] Gửi tin khởi động thành công")
                else:
                    body = await resp.text()
                    print("=" * 50)
                    print(f"  [LỖI TELEGRAM] HTTP {resp.status} ({label})")
                    print(f"  Chi tiết: {body}")
                    print(f"  Kiểm tra lại TELEGRAM_TOKEN và chat ID cho {label}")
                    print("=" * 50)
                    logger.error(f"[TG-{label}] Lỗi {resp.status}: {body}")
    except Exception as e:
        print("=" * 50)
        print(f"  [LỖI TELEGRAM] Không kết nối được ({label}): {e}")
        print("  Kiểm tra lại token và chat ID")
        print("=" * 50)
        logger.error(f"[TG-{label}] Không gửi được: {e}")


async def _send_startup_message() -> None:
    watch_desc = 'Top ' + str(TOP_SYMBOLS_COUNT) + ' coin' if AUTO_TOP_SYMBOLS else str(len(SYMBOLS)) + ' coin'

    text_new = (
        "✅ *Bot đã khởi động*\n\n"
        f"Timeframe: `{INTERVAL_H1_DISPLAY}`\n"
        f"Theo dõi: `{watch_desc}`\n"
        f"Cooldown: `{ALERT_COOLDOWN_MINUTES} phút`\n"
        f"TP/SL: `{NEW_TP_PCT*100:.1f}% / {NEW_SL_PCT*100:.1f}%`\n\n"
        "Đang chờ tín hiệu LONG + SHORT (chạm BB)..."
    )
    await _send_startup_message_to(TELEGRAM_CHAT_ID, "NEW-H1", text_new)

    text_legacy = (
        "✅ *Bot đã khởi động*\n\n"
        f"Timeframe: `{INTERVAL_H1_DISPLAY}`\n"
        f"Theo dõi: `{watch_desc}`\n"
        f"Cooldown: `{ALERT_COOLDOWN_MINUTES} phút`\n"
        f"TP/SL: `{LEGACY_TP_PCT*100:.1f}% / {LEGACY_SL_PCT*100:.1f}%`\n\n"
        "Đang chờ tín hiệu SHORT (cũ)..."
    )
    await _send_startup_message_to(TELEGRAM_CHAT_ID_H1, "LEGACY-SHORT-H1", text_legacy)


async def _run_forever(label: str, scanner) -> None:
    """Chạy 1 scanner vô hạn; nếu crash bất ngờ thì log + khởi động lại thay vì
    kéo sập luôn scanner còn lại (asyncio.gather sẽ hủy toàn bộ nếu 1 task raise)."""
    while True:
        try:
            await scanner.run()
        except Exception as e:
            logger.critical(f"[{label}] Scanner dừng bất ngờ, khởi động lại sau 10s: {e}", exc_info=True)
            await asyncio.sleep(10)


async def _main() -> None:
    _banner()
    await _send_startup_message()
    try:
        symbols = await resolve_symbols()
        await asyncio.gather(
            _run_forever("LONG-H1", Scanner(
                symbols, INTERVAL_H1, INTERVAL_H1_DISPLAY, TELEGRAM_CHAT_ID,
                detect_fn=lambda s, c: detect_signal(s, c, direction="LONG"),
                tp_pct=NEW_TP_PCT, sl_pct=NEW_SL_PCT,
            )),
            _run_forever("SHORT-H1", Scanner(
                symbols, INTERVAL_H1, INTERVAL_H1_DISPLAY, TELEGRAM_CHAT_ID,
                detect_fn=lambda s, c: detect_signal(s, c, direction="SHORT"),
                tp_pct=NEW_TP_PCT, sl_pct=NEW_SL_PCT,
                poll_offset_sec=H1_POLL_STAGGER_SEC,
            )),
            _run_forever("LEGACY-SHORT-H1", Scanner(
                symbols, INTERVAL_H1, INTERVAL_H1_DISPLAY, TELEGRAM_CHAT_ID_H1,
                detect_fn=detect_legacy_signal,
                tp_pct=LEGACY_TP_PCT, sl_pct=LEGACY_SL_PCT,
                message_builder=_build_legacy_message,
                poll_offset_sec=H1_POLL_STAGGER_SEC * 2,
            )),
        )
    except KeyboardInterrupt:
        logger.info("Bot dừng.")
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(_main())
