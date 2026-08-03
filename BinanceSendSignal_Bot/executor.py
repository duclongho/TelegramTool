#!/usr/bin/env python3
"""
Executor tu dong dat lenh len Binance Futures (mac dinh: DEMO TRADING) dua theo
tin hieu LONG/SHORT DOT BIEN (Spike) tu longH4Future.py.

Binance da khai tu testnet.binancefuture.com (trang cu, dang nhap bang GitHub),
thay bang he thong moi "Demo Trading" dung chung tai khoan Binance that, tai
demo.binance.com. Executor nay dung tham so demo=True cua python-binance de goi
dung endpoint moi (demo-fapi.binance.com), khong phai testnet=True (da lac hau).

An toan mac dinh: ENABLE_AUTO_TRADE = False -> khong dat lenh gi ca, chi log.
Chi bat khi da co API key Demo Trading va da doc ky phan CAU HINH ben duoi.
"""
import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_DOWN
from typing import Literal, Optional

from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException

logger = logging.getLogger("executor")

# ═══════════════════════════════════════════════
#  CẤU HÌNH — chỉnh ở đây
# ═══════════════════════════════════════════════
ENABLE_AUTO_TRADE = True   # BẬT = True để bot THỰC SỰ đặt lệnh. Mặc định TẮT để an toàn.
USE_DEMO_TRADING  = True    # True = Demo Trading (tiền ảo, endpoint demo-fapi.binance.com). False = tiền THẬT — cân nhắc kỹ.

# Lấy tại: đăng nhập binance.com bằng tài khoản Binance THẬT của bạn (không phải tài khoản
# riêng nữa) -> vào mục Demo Trading (thường ở Futures hoặc trong API Management) -> tạo
# "Demo Trading API Key". Đây là key riêng cho môi trường ảo, KHÔNG dùng chung với key thật.
DEMO_API_KEY    = "l1vyFwTdZBKfYioAcYHFhWU8U38G4rwcmV2isuyqUq3XCWKQ5Knsg8XUSLSd7Ve2"
DEMO_API_SECRET = "nABYNYgqZmbGyq4VkxGqHPz31D08Vx1KIzkUI1KEPmU3nJtI5GpHFikV0VXz4IBB"

RISK_PCT_PER_TRADE = 0.01   # 1% vốn khả dụng / lệnh, tính theo khoảng cách entry -> SL
LEVERAGE           = 3      # Đòn bẩy mặc định cho mọi symbol
MARGIN_TYPE         = "ISOLATED"   # ISOLATED | CROSSED

# Lệnh vào chỉ khớp nếu giá thị trường CHƯA trượt quá xa so với giá lúc phát tín hiệu
# (đặt bằng LIMIT + IOC thay vì MARKET) — khớp gần giá tín hiệu nhất có thể, và thà
# KHÔNG khớp còn hơn đuổi theo giá đã chạy quá xa trong lúc đột biến.
MAX_ENTRY_SLIPPAGE_PCT = 0.0015   # 0.15%

MAX_CONCURRENT_POSITIONS = 5      # Số lệnh mở đồng thời tối đa (toàn bộ executor, không phải mỗi symbol)
DAILY_LOSS_LIMIT_PCT     = 0.5   # Lỗ ròng trong ngày >= 50% vốn lúc đầu ngày -> tự tắt auto-trade

# Dùng làm SL/TP mặc định khi phát hiện 1 vị thế đã mở sẵn lúc khởi động (crash/restart/
# đặt tay) mà không biết % SL/TP gốc lúc mở — nên khớp với SPIKE_SL_PCT/SPIKE_TP_PCT
# của kèo đột biến trong longH4Future.py.
DEFAULT_SL_PCT = 0.02
DEFAULT_TP_PCT = 0.025

# Tạo 1 file rỗng tên này (cùng thư mục chạy bot) để DỪNG KHẨN CẤP việc mở lệnh mới
# (lệnh đang mở không bị đóng, chỉ chặn mở thêm) — xoá file để chạy lại bình thường.
EMERGENCY_STOP_FILE = "EXECUTOR_STOP"


@dataclass
class _SymbolFilters:
    step_size:   Decimal
    tick_size:   Decimal
    min_qty:     Decimal
    min_notional: Decimal


def _round_step(value: float, step: Decimal) -> float:
    """Làm tròn XUỐNG theo bội số của step (stepSize/tickSize) — bắt buộc với Binance,
    gửi số không đúng bội số sẽ bị từ chối lệnh (lỗi -1111 precision)."""
    d = Decimal(str(value))
    return float((d // step) * step)


class TradeExecutor:
    """Đặt lệnh MARKET vào lệnh + STOP_MARKET/TAKE_PROFIT_MARKET (closePosition=True) để
    thoát lệnh, theo dõi khớp lệnh qua User Data Stream để dọn lệnh còn treo + tính PnL ngày."""

    def __init__(self, client: AsyncClient, notify) -> None:
        self.client = client
        self.notify = notify   # async callable(str) -> gửi Telegram (dùng lại send_via_extra_bot kiểu có sẵn)
        self._filters_cache: dict[str, _SymbolFilters] = {}
        self._leverage_set: set[str] = set()
        self._open_positions: dict[str, dict] = {}   # symbol -> {"direction":..., "qty":...}
        self._daily_pnl       = 0.0
        self._daily_pnl_date  = date.today()
        self._equity_day_start: Optional[float] = None
        self.auto_trade_enabled = True   # có thể bị tự tắt khi chạm daily loss limit
        self._cached_balance: Optional[float] = None   # cập nhật cục bộ, tránh gọi REST mỗi lần vào lệnh

    @classmethod
    async def create(cls, notify) -> "TradeExecutor":
        # demo=True (KHÔNG phải testnet=True — đó là endpoint cũ đã bị Binance khai tử)
        client = await AsyncClient.create(
            DEMO_API_KEY, DEMO_API_SECRET, demo=USE_DEMO_TRADING,
        )
        self = cls(client, notify)
        await self._preload_filters()
        self._cached_balance = await self._fetch_balance()
        return self

    async def close(self) -> None:
        await self.client.close_connection()

    # --- TIỆN ÍCH ---
    @staticmethod
    def _parse_filters(filters: list[dict]) -> _SymbolFilters:
        step = tick = min_qty = min_notional = None
        for f in filters:
            if f["filterType"] == "LOT_SIZE":
                step    = Decimal(f["stepSize"])
                min_qty = Decimal(f["minQty"])
            elif f["filterType"] == "PRICE_FILTER":
                tick = Decimal(f["tickSize"])
            elif f["filterType"] in ("MIN_NOTIONAL", "NOTIONAL"):
                min_notional = Decimal(f.get("notional", f.get("minNotional", "5")))
        return _SymbolFilters(
            step_size=step or Decimal("1"),
            tick_size=tick or Decimal("0.01"),
            min_qty=min_qty or Decimal("0"),
            min_notional=min_notional or Decimal("5"),
        )

    async def _preload_filters(self) -> None:
        """Tải trước step/tick size của TOÀN BỘ symbol trong 1 lần gọi duy nhất lúc khởi
        động — tránh việc lần vào lệnh ĐẦU TIÊN của mỗi symbol bị chậm thêm vì phải gọi
        exchangeInfo ngay lúc đó (đúng lúc giá đang biến động mạnh, càng chậm càng trượt giá)."""
        info = await self.client.futures_exchange_info()
        for s in info["symbols"]:
            self._filters_cache[s["symbol"]] = self._parse_filters(s["filters"])
        logger.info(f"[Executor] Đã tải trước filters cho {len(self._filters_cache)} symbol")

    async def _get_filters(self, symbol: str) -> _SymbolFilters:
        cached = self._filters_cache.get(symbol)
        if cached:
            return cached
        # Hiếm khi xảy ra (symbol mới niêm yết sau lúc preload) — gọi lại riêng cho symbol đó
        info = await self.client.futures_exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                filters = self._parse_filters(s["filters"])
                self._filters_cache[symbol] = filters
                return filters
        raise ValueError(f"Không tìm thấy symbol {symbol} trong exchangeInfo")

    async def _get_position(self, symbol: str) -> tuple[float, float]:
        """Trả về (|positionAmt|, entryPrice) THẬT trên sàn cho symbol — dùng làm nguồn
        xác nhận chính khi vào lệnh, vì phản hồi/lookup của lệnh riêng lẻ đã chứng minh
        không đáng tin cậy trên Demo Trading (xem ghi chú trong open_position)."""
        positions = await self.client.futures_position_information(symbol=symbol)
        if not positions:
            return 0.0, 0.0
        p = positions[0]
        return abs(float(p["positionAmt"])), float(p["entryPrice"])

    async def _fetch_balance(self) -> float:
        balances = await self.client.futures_account_balance()
        for b in balances:
            if b["asset"] == "USDT":
                return float(b["availableBalance"])
        return 0.0

    async def _get_available_balance(self) -> float:
        """Dùng số dư cache cục bộ (cập nhật lúc mở/đóng lệnh) thay vì gọi REST mỗi lần
        có tín hiệu — bớt 1 lượt round-trip mạng khỏi đường găng ngay trước khi đặt lệnh,
        giúp khớp giá gần với giá tín hiệu hơn."""
        if self._cached_balance is None:
            self._cached_balance = await self._fetch_balance()
        return self._cached_balance

    async def _ensure_leverage(self, symbol: str) -> None:
        if symbol in self._leverage_set:
            return
        try:
            await self.client.futures_change_margin_type(symbol=symbol, marginType=MARGIN_TYPE)
        except BinanceAPIException as e:
            if e.code != -4046:   # -4046 = "No need to change margin type" -> bỏ qua, không phải lỗi
                logger.warning(f"[Executor] Đổi margin type {symbol} lỗi: {e}")
        try:
            await self.client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
        except BinanceAPIException as e:
            logger.error(f"[Executor] Đặt đòn bẩy {symbol} lỗi: {e}")
        self._leverage_set.add(symbol)

    def _reset_daily_if_needed(self) -> None:
        today = date.today()
        if today != self._daily_pnl_date:
            self._daily_pnl_date  = today
            self._daily_pnl       = 0.0
            self._equity_day_start = None
            if not self.auto_trade_enabled:
                self.auto_trade_enabled = True
                logger.info("[Executor] Sang ngày mới — bật lại auto-trade (đã bị tắt do chạm giới hạn lỗ ngày hôm qua)")

    # --- NHẬN THEO DÕI VỊ THẾ ĐÃ MỞ SẴN LÚC KHỞI ĐỘNG ---
    async def reconcile_on_startup(self, symbols: set[str]) -> None:
        """Gọi 1 lần lúc khởi động: quét vị thế đang mở thật trên sàn cho các symbol bot
        theo dõi, đưa vào _open_positions để tiếp tục theo dõi (đặt bổ sung SL/TP nếu
        thiếu). Bắt buộc phải có bước này — nếu bot khởi động lại (crash/restart) hoặc có
        vị thế mở tay/test từ trước, _open_positions rỗng sẽ khiến bot KHÔNG BAO GIỜ phát
        hiện được khi các vị thế đó đóng (đã xác nhận qua thực tế khi debug executor này)."""
        if not ENABLE_AUTO_TRADE:
            return
        try:
            positions = await self.client.futures_position_information()
        except Exception as e:
            logger.error(f"[Executor] Lỗi quét vị thế lúc khởi động: {e}")
            return

        algo_orders: list[dict] = []
        try:
            algo_orders = await self.client.futures_get_open_algo_orders()
        except Exception as e:
            logger.error(f"[Executor] Lỗi lấy algo orders lúc khởi động: {e}")

        adopted = []
        for p in positions:
            symbol = p["symbol"]
            amt = float(p.get("positionAmt", 0))
            if amt == 0 or symbol not in symbols:
                continue

            direction   = "LONG" if amt > 0 else "SHORT"
            entry_price = float(p["entryPrice"])
            close_side  = "SELL" if direction == "LONG" else "BUY"

            sym_algo = [o for o in algo_orders if o["symbol"] == symbol]
            has_sl = any(o["orderType"] == "STOP_MARKET" for o in sym_algo)
            has_tp = any(o["orderType"] == "TAKE_PROFIT_MARKET" for o in sym_algo)

            if not (has_sl and has_tp):
                logger.warning(f"[Executor] {symbol}: vị thế có sẵn THIẾU SL/TP — đặt bổ sung "
                                f"(dùng {DEFAULT_SL_PCT*100:.1f}%/{DEFAULT_TP_PCT*100:.1f}% mặc định)")
                try:
                    filters = await self._get_filters(symbol)
                    if direction == "LONG":
                        sl_price = entry_price * (1 - DEFAULT_SL_PCT)
                        tp_price = entry_price * (1 + DEFAULT_TP_PCT)
                    else:
                        sl_price = entry_price * (1 + DEFAULT_SL_PCT)
                        tp_price = entry_price * (1 - DEFAULT_TP_PCT)
                    if not has_sl:
                        await self.client.futures_create_order(
                            symbol=symbol, side=close_side, type="STOP_MARKET",
                            stopPrice=_round_step(sl_price, filters.tick_size), closePosition=True,
                        )
                    if not has_tp:
                        await self.client.futures_create_order(
                            symbol=symbol, side=close_side, type="TAKE_PROFIT_MARKET",
                            stopPrice=_round_step(tp_price, filters.tick_size), closePosition=True,
                        )
                except Exception as e:
                    logger.error(f"[Executor] {symbol}: đặt SL/TP bổ sung lúc khởi động lỗi: {e}")

            self._open_positions[symbol] = {
                "direction": direction, "qty": abs(amt),
                "margin": abs(amt) * entry_price / LEVERAGE,
                "opened_at": int(time.time() * 1000),
            }
            adopted.append(f"{direction} {symbol} {abs(amt)}@{entry_price}")

        if adopted:
            logger.warning(f"[Executor] Đã nhận theo dõi {len(adopted)} vị thế có sẵn: {', '.join(adopted)}")
            await self.notify(
                f"🔄 [DEMO] Khởi động — đã nhận theo dõi {len(adopted)} vị thế đang mở sẵn:\n"
                + "\n".join(adopted)
            )

    # --- MỞ LỆNH ---
    async def open_position(self, symbol: str, direction: Literal["LONG", "SHORT"],
                             entry_price: float, sl_price: float, tp_price: float,
                             sl_pct: float, tp_pct: float) -> None:
        if not ENABLE_AUTO_TRADE:
            return
        self._reset_daily_if_needed()

        if os.path.exists(EMERGENCY_STOP_FILE):
            logger.info(f"[Executor] {symbol}: bỏ qua — đang ở chế độ DỪNG KHẨN CẤP ({EMERGENCY_STOP_FILE} tồn tại)")
            return
        if not self.auto_trade_enabled:
            logger.info(f"[Executor] {symbol}: bỏ qua — auto-trade đang tắt (đã chạm giới hạn lỗ ngày)")
            return
        if symbol in self._open_positions:
            return   # đã có lệnh mở cho symbol này
        if len(self._open_positions) >= MAX_CONCURRENT_POSITIONS:
            logger.info(f"[Executor] {symbol}: bỏ qua — đã đạt tối đa {MAX_CONCURRENT_POSITIONS} lệnh mở đồng thời")
            return

        try:
            balance = await self._get_available_balance()
            if self._equity_day_start is None:
                self._equity_day_start = balance

            sl_distance = abs(entry_price - sl_price)
            if sl_distance <= 0:
                return
            risk_amount = balance * RISK_PCT_PER_TRADE
            raw_qty     = risk_amount / sl_distance

            filters = await self._get_filters(symbol)
            qty = _round_step(raw_qty, filters.step_size)
            if qty < float(filters.min_qty) or qty <= 0:
                logger.info(f"[Executor] {symbol}: vốn quá nhỏ để mở lệnh đạt minQty ({filters.min_qty}), bỏ qua")
                return
            if qty * entry_price < float(filters.min_notional):
                logger.info(f"[Executor] {symbol}: khối lượng {qty} không đạt notional tối thiểu, bỏ qua")
                return

            margin_required = qty * entry_price / LEVERAGE
            if margin_required > balance:
                logger.info(f"[Executor] {symbol}: không đủ margin khả dụng ({balance:.2f} USDT), bỏ qua")
                return

            await self._ensure_leverage(symbol)

            side          = "BUY" if direction == "LONG" else "SELL"
            opposite_side = "SELL" if direction == "LONG" else "BUY"

            # LIMIT + IOC thay vì MARKET: giới hạn mức trượt giá tối đa cho phép
            # (MAX_ENTRY_SLIPPAGE_PCT) — nếu giá đã chạy quá xa so với lúc phát tín hiệu,
            # lệnh sẽ KHÔNG khớp thay vì đuổi mua/bán bằng mọi giá.
            limit_price = (entry_price * (1 + MAX_ENTRY_SLIPPAGE_PCT) if direction == "LONG"
                           else entry_price * (1 - MAX_ENTRY_SLIPPAGE_PCT))
            limit_price = _round_step(limit_price, filters.tick_size)

            # QUAN TRỌNG: KHÔNG dùng phản hồi của futures_create_order (executedQty) hay
            # truy vấn lại qua futures_get_order để xác nhận khớp — đã xác nhận thực tế
            # trên Demo Trading là CẢ HAI đều có thể sai lệch (báo 0/"không tồn tại" dù
            # lệnh đã khớp thật, do độ trễ đồng bộ dữ liệu giữa các endpoint). Xác nhận
            # duy nhất đáng tin cậy là so sánh TRẠNG THÁI VỊ THẾ THẬT trước/sau khi đặt.
            pos_amt_before, _ = await self._get_position(symbol)

            await self.client.futures_create_order(
                symbol=symbol, side=side, type="LIMIT", timeInForce="IOC",
                price=limit_price, quantity=qty,
            )

            await asyncio.sleep(0.8)
            pos_amt_after, pos_entry_price = await self._get_position(symbol)
            filled_qty = round(abs(pos_amt_after - pos_amt_before), 8)
            if filled_qty <= 0:
                # Không khớp -> bỏ qua kèo âm thầm, không báo Telegram (chỉ log nội bộ)
                logger.info(f"[Executor] {symbol}: lệnh IOC không khớp — giá đã trượt quá "
                            f"{MAX_ENTRY_SLIPPAGE_PCT*100:.2f}% so với giá tín hiệu, bỏ qua")
                return

            # Giá vào lệnh THỰC TẾ = entryPrice thật của vị thế sau khi khớp (không phải
            # giá lúc phát tín hiệu) — do lệnh đã bị giới hạn trượt giá ở trên, mức lệch
            # này giờ chỉ còn trong khoảng ±MAX_ENTRY_SLIPPAGE_PCT.
            actual_entry = pos_entry_price or limit_price
            slippage_pct = (actual_entry - entry_price) / entry_price * 100
            partial_note = "" if filled_qty >= qty * 0.999 else f" (khớp một phần {filled_qty}/{qty})"
            logger.info(f"[Executor] {symbol} {direction} MỞ LỆNH qty={filled_qty}{partial_note} "
                        f"giá tín hiệu={entry_price} "
                        f"giá khớp thực tế={actual_entry} (lệch {slippage_pct:+.2f}%)")

            actual_margin = filled_qty * actual_entry / LEVERAGE   # theo qty THỰC KHỚP, không phải qty đặt
            self._cached_balance = balance - actual_margin

            # Tính lại SL/TP theo giá khớp THỰC TẾ (không dùng giá lúc phát tín hiệu),
            # để đúng đúng % rủi ro/chốt lời đã cấu hình dù giá vào có bị trượt.
            if direction == "LONG":
                sl_price = actual_entry * (1 - sl_pct)
                tp_price = actual_entry * (1 + tp_pct)
            else:
                sl_price = actual_entry * (1 + sl_pct)
                tp_price = actual_entry * (1 - tp_pct)

            sl_rounded = _round_step(sl_price, filters.tick_size)
            tp_rounded = _round_step(tp_price, filters.tick_size)

            await self.client.futures_create_order(
                symbol=symbol, side=opposite_side, type="STOP_MARKET",
                stopPrice=sl_rounded, closePosition=True,
            )
            await self.client.futures_create_order(
                symbol=symbol, side=opposite_side, type="TAKE_PROFIT_MARKET",
                stopPrice=tp_rounded, closePosition=True,
            )

            self._open_positions[symbol] = {
                "direction": direction, "qty": filled_qty, "margin": actual_margin,
                "opened_at": int(time.time() * 1000),
            }
            await self.notify(
                f"🤖 [DEMO] Đã mở {direction} {symbol} qty={filled_qty}{partial_note}\n"
                f"Giá tín hiệu: {entry_price} → Giá khớp thực tế: {actual_entry} (lệch {slippage_pct:+.2f}%)\n"
                f"TP={tp_rounded} SL={sl_rounded}"
            )
        except BinanceAPIException as e:
            logger.error(f"[Executor] {symbol}: Binance API lỗi khi mở lệnh: {e}")
        except Exception as e:
            logger.error(f"[Executor] {symbol}: lỗi không xác định khi mở lệnh: {e}", exc_info=True)

    # --- DỌN DẸP + BÁO ĐÓNG LỆNH (dùng chung cho cả 2 nguồn phát hiện bên dưới) ---
    async def _apply_close(self, symbol: str, direction: str, result: str,
                            realized_pnl: float, note: str = "") -> None:
        self._daily_pnl += realized_pnl
        if self._cached_balance is not None:
            closed = self._open_positions.get(symbol, {})
            self._cached_balance += closed.get("margin", 0.0) + realized_pnl
        self._open_positions.pop(symbol, None)

        icon = "✅" if result == "TP" else ("🛑" if result == "SL" else "ℹ️")
        await self.notify(
            f"{icon} [DEMO] Đóng {direction} {symbol} [{result}]{note} "
            f"| PnL: {realized_pnl:+.2f} USDT | PnL ngày: {self._daily_pnl:+.2f} USDT"
        )

        if (self._equity_day_start and self.auto_trade_enabled and
                self._daily_pnl <= -DAILY_LOSS_LIMIT_PCT * self._equity_day_start):
            self.auto_trade_enabled = False
            await self.notify(
                f"⛔ [DEMO] Đã TẮT auto-trade — lỗ ngày chạm giới hạn "
                f"{DAILY_LOSS_LIMIT_PCT*100:.0f}% vốn ({self._daily_pnl:+.2f} USDT)"
            )

    async def _cancel_leftover_orders(self, symbol: str) -> None:
        """SL/TP đặt với closePosition=True trên Demo Trading được Binance xử lý như
        ALGO ORDER (không phải lệnh thường) -> phải huỷ bằng futures_cancel_all_algo_open_orders,
        futures_cancel_all_open_orders KHÔNG huỷ được loại này (đã xác nhận thực tế)."""
        try:
            await self.client.futures_cancel_all_algo_open_orders(symbol=symbol)
        except BinanceAPIException as e:
            logger.warning(f"[Executor] {symbol}: huỷ algo order còn lại lỗi: {e}")
        try:
            await self.client.futures_cancel_all_open_orders(symbol=symbol)
        except BinanceAPIException:
            pass   # không có lệnh thường nào để huỷ -> bỏ qua, không phải lỗi

    # --- THEO DÕI KHỚP LỆNH (User Data Stream) ---
    async def _handle_order_update(self, order: dict) -> None:
        symbol = order.get("s")
        status = order.get("X")
        otype  = order.get("o")
        if symbol not in self._open_positions:
            return
        if status != "FILLED" or otype not in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            return

        realized_pnl = float(order.get("rp", 0.0))
        result    = "TP" if otype == "TAKE_PROFIT_MARKET" else "SL"
        direction = self._open_positions.get(symbol, {}).get("direction", "?")

        await self._cancel_leftover_orders(symbol)
        await self._apply_close(symbol, direction, result, realized_pnl)

    async def run_user_data_stream(self) -> None:
        """Chạy vô hạn, tự kết nối lại nếu WS rớt — gọi bằng asyncio.create_task từ _main()."""
        if not ENABLE_AUTO_TRADE:
            return
        bsm = BinanceSocketManager(self.client)
        while True:
            try:
                async with bsm.futures_user_socket() as stream:
                    logger.info("[Executor] User Data Stream kết nối OK")
                    while True:
                        msg = await stream.recv()
                        if msg.get("e") == "ORDER_TRADE_UPDATE":
                            await self._handle_order_update(msg["o"])
            except Exception as e:
                logger.error(f"[Executor] User Data Stream lỗi, kết nối lại sau 5s: {e}")
                await asyncio.sleep(5)

    # --- LƯỚI AN TOÀN DỰ PHÒNG (không phụ thuộc WS) ---
    RECONCILE_INTERVAL_SEC = 10

    async def _handle_position_closed_externally(self, symbol: str) -> None:
        """Vị thế đã về 0 trên sàn nhưng KHÔNG nhận được ORDER_TRADE_UPDATE tương ứng
        (có thể do algo order không bắn đúng event qua User Data Stream ở Demo Trading) —
        phát hiện qua kiểm tra định kỳ, lấy PnL thật từ income history để báo cho đúng."""
        pos = self._open_positions.get(symbol)
        if pos is None:
            return
        direction = pos.get("direction", "?")

        await self._cancel_leftover_orders(symbol)

        realized_pnl = 0.0
        try:
            income = await self.client.futures_income_history(
                symbol=symbol, incomeType="REALIZED_PNL",
                startTime=pos.get("opened_at", 0), limit=50,
            )
            realized_pnl = sum(float(i["income"]) for i in income)
        except Exception as e:
            logger.warning(f"[Executor] {symbol}: không lấy được realized PnL từ income history: {e}")

        await self._apply_close(symbol, direction, "TP/SL", realized_pnl, note=" (qua kiểm tra định kỳ)")

    async def run_reconciliation_loop(self) -> None:
        """Kiểm tra định kỳ vị thế qua REST — lưới an toàn dự phòng, độc lập với WS.
        Gọi song song với run_user_data_stream() từ _main()."""
        if not ENABLE_AUTO_TRADE:
            return
        while True:
            await asyncio.sleep(self.RECONCILE_INTERVAL_SEC)
            for symbol in list(self._open_positions.keys()):
                try:
                    positions = await self.client.futures_position_information(symbol=symbol)
                    amt = float(positions[0]["positionAmt"]) if positions else 0.0
                    if amt == 0:
                        await self._handle_position_closed_externally(symbol)
                except Exception as e:
                    logger.error(f"[Executor] {symbol}: lỗi kiểm tra định kỳ: {e}")
