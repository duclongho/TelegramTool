#!/usr/bin/env python3
"""
Bo test cho longH4Future.py - 1000 test case kiem tra logic phat hien tin hieu
va tinh toan du lieu (Bollinger Band, TP/SL).

Cach chay: python test_signals.py
Khong can ket noi mang - toan bo test dung du lieu nen gia lap (synthetic).
"""
import importlib.util
import math
import os
import random

random.seed(1234)  # cho ket qua lap lai duoc

_HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("bot", os.path.join(_HERE, "longH4Future.py"))
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)

bot.logger.disabled = True  # tat log cua bot de output test khong bi lan

# ═══════════════════════════════════════════════
#  BO DEM KET QUA
# ═══════════════════════════════════════════════
results = {"pass": 0, "fail": 0}
failures: list[str] = []


def check(category: str, idx: int, condition: bool, detail: str = "") -> None:
    if condition:
        results["pass"] += 1
    else:
        results["fail"] += 1
        failures.append(f"[{category} #{idx}] {detail}")


def mk(open_, high, low, close, volume):
    return {"open": open_, "high": high, "low": low, "close": close, "volume": volume}


def make_base(n: int, start_price: float = 100.0, step: float = 0.02, vol: float = 100.0):
    candles = []
    price = start_price
    for _ in range(n):
        candles.append(mk(price, price + 0.1, price - 0.1, price + step, vol))
        price += step
    return candles


BASE_LEN = bot.BB_PERIOD + 10

# ═══════════════════════════════════════════════
#  1) detect_signal LONG (Chuon chuon / Dragonfly)  -> 100 valid + 100 invalid
# ═══════════════════════════════════════════════
CATEGORY = "LONG-doji"
for i in range(200):
    base = make_base(BASE_LEN, start_price=random.uniform(1, 5000), step=random.uniform(-0.05, 0.05))
    ind = bot.compute_indicators(base)
    if ind["bb_middle"] == 0.0:
        continue
    bb_lower, bb_middle = ind["bb_lower"], ind["bb_middle"]
    gap = bb_middle - bb_lower
    if gap <= 0:
        continue

    valid = i < 100
    violate = None if valid else random.choice(
        ["color", "wick", "cross", "middle", "volume", "body"])

    rng = random.uniform(0.1, 0.6) * gap
    cross_frac = random.uniform(0.15, 0.95)
    upper_wick_frac = random.uniform(0.0, 0.08)
    body_frac = random.uniform(0.0, 0.25)
    vol_mult = random.uniform(2.1, 8.0)

    if violate == "cross":
        cross_frac = random.uniform(-0.5, 0.0)   # low khong xuyen qua bb_lower nua (hoac chi cham)
    if violate == "wick":
        upper_wick_frac = random.uniform(0.15, 0.4)   # rau tren qua dai
    if violate == "body":
        body_frac = random.uniform(0.4, 0.8)   # than qua to, khong con la doji
    if violate == "volume":
        vol_mult = random.uniform(0.2, 1.9)   # khong du gap doi

    low = bb_lower - cross_frac * rng
    high = low + rng
    if violate == "middle":
        # day nen len vuot qua bb_middle
        high = bb_middle + random.uniform(0.05, gap)
        low = high - rng

    upper_wick = upper_wick_frac * rng
    body = body_frac * rng
    close = high - upper_wick
    open_ = close - body   # close > open => nen xanh

    if violate == "color":
        open_, close = close, open_   # dao nguoc -> nen do

    prev_vol = 100.0
    volume = vol_mult * prev_vol

    curr = mk(open_, high, low, close, volume)
    candles = base[:]
    candles[-1] = mk(candles[-1]["open"], candles[-1]["high"], candles[-1]["low"], candles[-1]["close"], prev_vol)
    candles.append(curr)

    result = bot.detect_signal("TEST", candles, direction="LONG")
    ok = (result is not None) if valid else (result is None)
    check(CATEGORY, i, ok, f"valid={valid} violate={violate} result={result is not None}")

# ═══════════════════════════════════════════════
#  2) detect_signal SHORT (Bia mo / Gravestone)  -> 100 valid + 100 invalid
# ═══════════════════════════════════════════════
CATEGORY = "SHORT-doji"
for i in range(200):
    base = make_base(BASE_LEN, start_price=random.uniform(1, 5000), step=random.uniform(-0.05, 0.05))
    ind = bot.compute_indicators(base)
    if ind["bb_middle"] == 0.0:
        continue
    bb_upper, bb_middle = ind["bb_upper"], ind["bb_middle"]
    gap = bb_upper - bb_middle
    if gap <= 0:
        continue

    valid = i < 100
    violate = None if valid else random.choice(
        ["color", "wick", "cross", "middle", "volume", "body"])

    rng = random.uniform(0.1, 0.6) * gap
    cross_frac = random.uniform(0.15, 0.95)
    lower_wick_frac = random.uniform(0.0, 0.08)
    body_frac = random.uniform(0.0, 0.25)
    vol_mult = random.uniform(2.1, 8.0)

    if violate == "cross":
        cross_frac = random.uniform(-0.5, 0.0)
    if violate == "wick":
        lower_wick_frac = random.uniform(0.15, 0.4)
    if violate == "body":
        body_frac = random.uniform(0.4, 0.8)
    if violate == "volume":
        vol_mult = random.uniform(0.2, 1.9)

    high = bb_upper + cross_frac * rng
    low = high - rng
    if violate == "middle":
        low = bb_middle - random.uniform(0.05, gap)
        high = low + rng

    lower_wick = lower_wick_frac * rng
    body = body_frac * rng
    close = low + lower_wick
    open_ = close + body   # open > close => nen do

    if violate == "color":
        open_, close = close, open_   # dao nguoc -> nen xanh

    prev_vol = 100.0
    volume = vol_mult * prev_vol

    curr = mk(open_, high, low, close, volume)
    candles = base[:]
    candles[-1] = mk(candles[-1]["open"], candles[-1]["high"], candles[-1]["low"], candles[-1]["close"], prev_vol)
    candles.append(curr)

    result = bot.detect_signal("TEST", candles, direction="SHORT")
    ok = (result is not None) if valid else (result is None)
    check(CATEGORY, i, ok, f"valid={valid} violate={violate} result={result is not None}")

# ═══════════════════════════════════════════════
#  3) detect_legacy_signal  -> 100 valid + 100 invalid
# ═══════════════════════════════════════════════
CATEGORY = "legacy"
for i in range(200):
    base = make_base(bot.BB_PERIOD + 8, start_price=random.uniform(1, 5000), step=random.uniform(-0.05, 0.05))
    approx_mid = sum(c["close"] for c in base[-bot.BB_PERIOD:]) / bot.BB_PERIOD

    valid = i < 100
    violate = None if valid else random.choice(
        ["prev_color", "curr_color", "near_band", "straddle", "vol_ratio"])

    # Nen 1 (prev): xanh, dong cua gan bb_middle (trong dung sai)
    tol_frac = random.uniform(0.0, 0.5) * bot.BAND_TOUCH_TOLERANCE   # trong 50% dung sai cho phep -> co bien du
    if violate == "near_band":
        tol_frac = random.uniform(2.0, 5.0) * bot.BAND_TOUCH_TOLERANCE   # vuot xa dung sai

    prev_close = approx_mid * (1 + random.choice([-1, 1]) * tol_frac)
    prev_open = prev_close - random.uniform(0.01, 0.3)
    if violate == "prev_color":
        prev_open = prev_close + random.uniform(0.01, 0.3)   # open > close => chac chan la nen do

    prev_vol = 100.0
    prev = mk(prev_open, max(prev_open, prev_close) + 0.05, min(prev_open, prev_close) - 0.05, prev_close, prev_vol)

    history_with_prev = base[:-1] + [prev]
    ind = bot.compute_indicators(history_with_prev)
    mid = ind["bb_middle"]
    if mid == 0.0:
        continue

    # Nen 2 (curr): xanh, straddle qua bb_middle that (open<mid<close)
    delta = random.uniform(0.05, 1.0)
    curr_open = mid - delta
    curr_close = mid + delta
    if violate == "straddle":
        # khong con straddle: dong cua duoi/tren bien giua
        curr_open = mid + delta
        curr_close = mid + delta * 2
    if violate == "curr_color":
        curr_open, curr_close = curr_close, curr_open   # do

    vol_ratio = random.uniform(0.96, 1.14)
    if violate == "vol_ratio":
        vol_ratio = random.choice([random.uniform(0.3, 0.9), random.uniform(1.3, 2.0)])
    curr_vol = prev_vol * vol_ratio

    curr = mk(curr_open, max(curr_open, curr_close) + 0.05, min(curr_open, curr_close) - 0.05, curr_close, curr_vol)

    candles = history_with_prev + [curr]
    result = bot.detect_legacy_signal("TEST", candles)
    ok = (result is not None) if valid else (result is None)
    check(CATEGORY, i, ok, f"valid={valid} violate={violate} result={result is not None}")

# ═══════════════════════════════════════════════
#  4) detect_spike_signal SHORT + LONG  -> 50+50 valid, 50+50 invalid
# ═══════════════════════════════════════════════
for direction, CATEGORY in (("SHORT", "spike-SHORT"), ("LONG", "spike-LONG")):
    for i in range(100):
        base = make_base(bot.BB_PERIOD + bot.SPIKE_LOOKBACK + 5,
                          start_price=random.uniform(1, 5000), step=random.uniform(-0.05, 0.05))
        ind = bot.compute_indicators(base)
        band = ind["bb_upper"] if direction == "SHORT" else ind["bb_lower"]
        if band == 0.0:
            continue

        lookback = base[-bot.SPIKE_LOOKBACK:]
        avg_range = sum(c["high"] - c["low"] for c in lookback) / bot.SPIKE_LOOKBACK
        avg_vol = sum(c["volume"] for c in lookback) / bot.SPIKE_LOOKBACK

        valid = i < 50
        violate = None if valid else random.choice(["range", "volume", "pierce"])

        range_mult = random.uniform(bot.SPIKE_RANGE_MULT + 0.5, bot.SPIKE_RANGE_MULT * 2.5)
        vol_mult = random.uniform(bot.SPIKE_VOL_MULT + 0.5, bot.SPIKE_VOL_MULT * 2.5)
        pierce_margin = random.uniform(0.05, 2.0)   # xuyen qua band bao nhieu

        if violate == "range":
            range_mult = random.uniform(0.5, bot.SPIKE_RANGE_MULT - 0.5)
        if violate == "volume":
            vol_mult = random.uniform(0.5, bot.SPIKE_VOL_MULT - 0.5)
        if violate == "pierce":
            pierce_margin = -random.uniform(0.1, 3.0)   # khong xuyen qua band

        live_range = range_mult * avg_range if avg_range > 0 else random.uniform(1, 5)
        live_vol = vol_mult * avg_vol if avg_vol > 0 else random.uniform(500, 2000)

        if direction == "SHORT":
            high = band + pierce_margin
            low = high - live_range
        else:
            low = band - pierce_margin
            high = low + live_range

        open_ = (high + low) / 2
        close = open_
        live_candle = mk(open_, high, low, close, live_vol)

        result = bot.detect_spike_signal("TEST", base, live_candle, direction=direction)
        ok = (result is not None) if valid else (result is None)
        check(CATEGORY, i, ok, f"valid={valid} violate={violate} result={result is not None}")

# ═══════════════════════════════════════════════
#  5) _position_hit  -> 100 case (TP/SL/ca hai/khong gi, ca LONG+SHORT)
# ═══════════════════════════════════════════════
CATEGORY = "position-hit"
for i in range(100):
    direction = random.choice(["LONG", "SHORT"])
    entry = random.uniform(1, 5000)
    tp_pct = random.uniform(0.01, 0.05)
    sl_pct = random.uniform(0.01, 0.05)
    if direction == "LONG":
        tp = entry * (1 + tp_pct)
        sl = entry * (1 - sl_pct)
    else:
        tp = entry * (1 - tp_pct)
        sl = entry * (1 + sl_pct)

    pos = bot.Position(symbol="TEST", direction=direction, entry=entry, tp=tp, sl=sl, opened_at=None)

    scenario = random.choice(["tp_only", "sl_only", "both", "neither"])
    pad = entry * 0.001
    if direction == "LONG":
        if scenario == "tp_only":
            candle = mk(entry, tp + pad, entry - pad, tp + pad * 0.5, 100)
            expected = "TP"
        elif scenario == "sl_only":
            candle = mk(entry, entry + pad, sl - pad, sl - pad * 0.5, 100)
            expected = "SL"
        elif scenario == "both":
            close_ = random.choice([tp, sl])
            open_ = tp if close_ == sl else sl
            candle = mk(open_, tp + pad, sl - pad, close_, 100)
            bearish = candle["close"] <= candle["open"]
            expected = "SL" if bearish else "TP"
        else:
            mid = (tp + sl) / 2 if direction == "LONG" else (tp + sl) / 2
            candle = mk(mid, min(tp, entry + pad) - pad, max(sl, entry - pad) + pad, mid, 100)
            expected = None
    else:
        if scenario == "tp_only":
            candle = mk(entry, entry + pad, tp - pad, tp - pad * 0.5, 100)
            expected = "TP"
        elif scenario == "sl_only":
            candle = mk(entry, sl + pad, entry - pad, sl + pad * 0.5, 100)
            expected = "SL"
        elif scenario == "both":
            close_ = random.choice([tp, sl])
            open_ = tp if close_ == sl else sl
            candle = mk(open_, sl + pad, tp - pad, close_, 100)
            bearish = candle["close"] <= candle["open"]
            expected = "TP" if bearish else "SL"
        else:
            mid = (tp + sl) / 2
            candle = mk(mid, min(sl, entry + pad) - pad, max(tp, entry - pad) + pad, mid, 100)
            expected = None

    result = bot._position_hit(pos, candle)
    ok = result == expected
    check(CATEGORY, i, ok, f"dir={direction} scenario={scenario} expected={expected} got={result}")

# ═══════════════════════════════════════════════
#  6) compute_indicators / _calc_bb - du lieu bien (edge case)  -> 100 case
# ═══════════════════════════════════════════════
CATEGORY = "bb-data"
for i in range(100):
    kind = random.choice(["insufficient", "flat", "normal", "extreme_volatility"])

    if kind == "insufficient":
        n = random.randint(0, bot.BB_PERIOD - 1)
        candles = make_base(n)
        ind = bot.compute_indicators(candles)
        ok = ind["bb_upper"] == 0.0 and ind["bb_middle"] == 0.0 and ind["bb_lower"] == 0.0
        check(CATEGORY, i, ok, f"kind={kind} n={n} ind={ind}")

    elif kind == "flat":
        price = random.uniform(1, 5000)
        candles = [mk(price, price, price, price, 100) for _ in range(bot.BB_PERIOD + 3)]
        ind = bot.compute_indicators(candles)
        # gia khong doi -> std ~ 0 -> upper=middle=lower (dung isclose vi sai so lam tron float)
        ok = (math.isclose(ind["bb_upper"], price, rel_tol=1e-6)
              and math.isclose(ind["bb_middle"], price, rel_tol=1e-6)
              and math.isclose(ind["bb_lower"], price, rel_tol=1e-6))
        check(CATEGORY, i, ok, f"kind={kind} ind={ind} price={price}")

    elif kind == "normal":
        candles = make_base(bot.BB_PERIOD + 5, start_price=random.uniform(1, 5000),
                             step=random.uniform(-0.1, 0.1))
        ind = bot.compute_indicators(candles)
        # bat bien co ban: upper >= middle >= lower, va middle nam giua
        ok = (ind["bb_upper"] >= ind["bb_middle"] >= ind["bb_lower"])
        check(CATEGORY, i, ok, f"kind={kind} ind={ind}")

    else:  # extreme_volatility
        price = random.uniform(1, 5000)
        candles = []
        for _ in range(bot.BB_PERIOD + 3):
            jump = price * random.uniform(-0.5, 0.5)
            candles.append(mk(price, price + abs(jump), price - abs(jump), price + jump, 100))
            price = max(0.0001, price + jump)
        ind = bot.compute_indicators(candles)
        ok = (ind["bb_upper"] >= ind["bb_middle"] >= ind["bb_lower"]) and all(
            v == v for v in ind.values())  # khong NaN
        check(CATEGORY, i, ok, f"kind={kind} ind={ind}")

# ═══════════════════════════════════════════════
#  7) DU LIEU THAT tu Binance (neu co mang) - smoke test khong crash
# ═══════════════════════════════════════════════
CATEGORY = "real-data"
real_data_tested = 0
try:
    import urllib.request
    import ssl
    import json as _json

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    test_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]
    for sym in test_symbols:
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval=1h&limit=200"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            data = _json.loads(resp.read())
        candles = [mk(float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])) for k in data]

        # Chay tat ca detector qua tung cua so truot - chi kiem tra KHONG crash
        for end in range(bot.BB_PERIOD + bot.SPIKE_LOOKBACK + 2, len(candles)):
            window = candles[:end]
            try:
                bot.detect_signal(sym, window, direction="LONG")
                bot.detect_signal(sym, window, direction="SHORT")
                bot.detect_legacy_signal(sym, window)
                bot.detect_spike_signal(sym, window[:-1], window[-1], direction="LONG")
                bot.detect_spike_signal(sym, window[:-1], window[-1], direction="SHORT")
                real_data_tested += 1
            except Exception as e:
                check(CATEGORY, real_data_tested, False, f"{sym} idx={end} lỗi: {e}")
    check(CATEGORY, 0, real_data_tested > 0, f"đã chạy {real_data_tested} cửa sổ dữ liệu thật, không crash")
except Exception as e:
    print(f"[real-data] Bỏ qua (không có mạng hoặc lỗi fetch): {e}")

# ═══════════════════════════════════════════════
#  BAO CAO KET QUA
# ═══════════════════════════════════════════════
total = results["pass"] + results["fail"]
print("=" * 60)
print(f"TONG SO TEST CASE: {total}")
print(f"  PASS: {results['pass']}")
print(f"  FAIL: {results['fail']}")
if real_data_tested:
    print(f"  (+ {real_data_tested} cửa sổ dữ liệu thật đã chạy, không crash)")
print("=" * 60)

if failures:
    print(f"\nChi tiet {len(failures)} case FAIL (toi da hien 30 dong dau):")
    for f in failures[:30]:
        print(f"  - {f}")

import sys
sys.exit(0 if results["fail"] == 0 else 1)
