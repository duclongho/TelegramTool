//@version=6
indicator(title="UT Bot Alerts", overlay=true)

// Inputs
a       = input.int(1,      title="Key Value. 'This changes the sensitivity'")
c       = input.int(10,     title="ATR Period")
h       = input.bool(false, title="Signals from Heikin Ashi Candles")
sl_pts  = input.float(10.0, title="Stop Loss (giá)",      minval=0.1, step=0.1)
tp1_pts = input.float(5.0,  title="Take Profit 1 (giá)",  minval=0.1, step=0.1)
tp2_pts = input.float(10.0, title="Take Profit 2 (giá)",  minval=0.1, step=0.1)
tp3_pts = input.float(15.0, title="Take Profit 3 (giá)",  minval=0.1, step=0.1)

xATR  = ta.atr(c)
nLoss = a * xATR

src = h ? request.security(ticker.heikinashi(syminfo.tickerid), timeframe.period, close) : close

var float xATRTrailingStop = 0.0
xATRTrailingStop := src > nz(xATRTrailingStop[1]) and src[1] > nz(xATRTrailingStop[1]) ? math.max(nz(xATRTrailingStop[1]), src - nLoss) :
   src < nz(xATRTrailingStop[1]) and src[1] < nz(xATRTrailingStop[1]) ? math.min(nz(xATRTrailingStop[1]), src + nLoss) :
   src > nz(xATRTrailingStop[1]) ? src - nLoss : src + nLoss

var int pos = 0
pos := src[1] < nz(xATRTrailingStop[1]) and src > nz(xATRTrailingStop[1]) ? 1 :
   src[1] > nz(xATRTrailingStop[1]) and src < nz(xATRTrailingStop[1]) ? -1 : nz(pos[1], 0)

ema_val = ta.ema(src, 1)
above   = ta.crossover(ema_val, xATRTrailingStop)
below   = ta.crossover(xATRTrailingStop, ema_val)

buy  = src > xATRTrailingStop and above
sell = src < xATRTrailingStop and below

barbuy  = src > xATRTrailingStop
barsell = src < xATRTrailingStop

// --- THEO DÕI VỊ THẾ & SL/TP ---
var float tp1_level = na
var float tp2_level = na
var float tp3_level = na
var float sl_level  = na
var int   trade_dir = 0

// Chỉ vào lệnh mới khi không có lệnh nào đang chạy
if buy and trade_dir == 0
    trade_dir := 1
    tp1_level := close + tp1_pts
    tp2_level := close + tp2_pts
    tp3_level := close + tp3_pts
    sl_level  := close - sl_pts

if sell and trade_dir == 0
    trade_dir := -1
    tp1_level := close - tp1_pts
    tp2_level := close - tp2_pts
    tp3_level := close - tp3_pts
    sl_level  := close + sl_pts

// Kiểm tra TP/SL bị chạm (ưu tiên TP cao nhất trước)
long_tp3_hit  = trade_dir ==  1 and not na(tp3_level) and high >= tp3_level
long_tp2_hit  = trade_dir ==  1 and not na(tp2_level) and high >= tp2_level and not long_tp3_hit
long_tp1_hit  = trade_dir ==  1 and not na(tp1_level) and high >= tp1_level and not long_tp2_hit and not long_tp3_hit
long_sl_hit   = trade_dir ==  1 and not na(sl_level)  and low  <= sl_level

short_tp3_hit = trade_dir == -1 and not na(tp3_level) and low  <= tp3_level
short_tp2_hit = trade_dir == -1 and not na(tp2_level) and low  <= tp2_level and not short_tp3_hit
short_tp1_hit = trade_dir == -1 and not na(tp1_level) and low  <= tp1_level and not short_tp2_hit and not short_tp3_hit
short_sl_hit  = trade_dir == -1 and not na(sl_level)  and high >= sl_level

// Lưu mức giá bị chạm trước khi reset
var float hit_price = na
if long_tp3_hit  or short_tp3_hit  => hit_price := tp3_level
if long_tp2_hit  or short_tp2_hit  => hit_price := tp2_level
if long_tp1_hit  or short_tp1_hit  => hit_price := tp1_level
if long_sl_hit   or short_sl_hit   => hit_price := sl_level

any_hit = long_tp1_hit or long_tp2_hit or long_tp3_hit or long_sl_hit or
          short_tp1_hit or short_tp2_hit or short_tp3_hit or short_sl_hit

if any_hit
    trade_dir := 0
    tp1_level := na
    tp2_level := na
    tp3_level := na
    sl_level  := na

// --- HIỂN THỊ ---
plot(trade_dir ==  1 ? tp1_level : na, title="Long TP1",  color=color.new(color.green, 60), style=plot.style_linebr, linewidth=1)
plot(trade_dir ==  1 ? tp2_level : na, title="Long TP2",  color=color.new(color.green, 30), style=plot.style_linebr, linewidth=1)
plot(trade_dir ==  1 ? tp3_level : na, title="Long TP3",  color=color.green,                style=plot.style_linebr, linewidth=2)
plot(trade_dir ==  1 ? sl_level  : na, title="Long SL",   color=color.red,                  style=plot.style_linebr, linewidth=1)

plot(trade_dir == -1 ? tp1_level : na, title="Short TP1", color=color.new(color.green, 60), style=plot.style_linebr, linewidth=1)
plot(trade_dir == -1 ? tp2_level : na, title="Short TP2", color=color.new(color.green, 30), style=plot.style_linebr, linewidth=1)
plot(trade_dir == -1 ? tp3_level : na, title="Short TP3", color=color.green,                style=plot.style_linebr, linewidth=2)
plot(trade_dir == -1 ? sl_level  : na, title="Short SL",  color=color.red,                  style=plot.style_linebr, linewidth=1)

plotshape(buy,  title="Buy",  text="Buy",  style=shape.labelup,   location=location.belowbar, color=color.green, textcolor=color.white, size=size.tiny)
plotshape(sell, title="Sell", text="Sell", style=shape.labeldown, location=location.abovebar, color=color.red,   textcolor=color.white, size=size.tiny)

barcolor(barbuy  ? color.green : na)
barcolor(barsell ? color.red   : na)

// --- ALERTS ---
_t  = syminfo.ticker
_ex = syminfo.prefix
_tf = timeframe.period
_c  = str.tostring(close, "#.##")

if buy
    alert('{"signal":"UT Long","ticker":"' + _t + '","close":"' + _c + '","tp1":"' + str.tostring(tp1_level, "#.##") + '","tp2":"' + str.tostring(tp2_level, "#.##") + '","tp3":"' + str.tostring(tp3_level, "#.##") + '","sl":"' + str.tostring(sl_level, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)

if sell
    alert('{"signal":"UT Short","ticker":"' + _t + '","close":"' + _c + '","tp1":"' + str.tostring(tp1_level, "#.##") + '","tp2":"' + str.tostring(tp2_level, "#.##") + '","tp3":"' + str.tostring(tp3_level, "#.##") + '","sl":"' + str.tostring(sl_level, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)

if long_tp1_hit
    alert('{"signal":"UT Long TP1","ticker":"' + _t + '","close":"' + _c + '","hit_price":"' + str.tostring(hit_price, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)

if long_tp2_hit
    alert('{"signal":"UT Long TP2","ticker":"' + _t + '","close":"' + _c + '","hit_price":"' + str.tostring(hit_price, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)

if long_tp3_hit
    alert('{"signal":"UT Long TP3","ticker":"' + _t + '","close":"' + _c + '","hit_price":"' + str.tostring(hit_price, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)

if long_sl_hit
    alert('{"signal":"UT Long SL","ticker":"' + _t + '","close":"' + _c + '","hit_price":"' + str.tostring(hit_price, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)

if short_tp1_hit
    alert('{"signal":"UT Short TP1","ticker":"' + _t + '","close":"' + _c + '","hit_price":"' + str.tostring(hit_price, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)

if short_tp2_hit
    alert('{"signal":"UT Short TP2","ticker":"' + _t + '","close":"' + _c + '","hit_price":"' + str.tostring(hit_price, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)

if short_tp3_hit
    alert('{"signal":"UT Short TP3","ticker":"' + _t + '","close":"' + _c + '","hit_price":"' + str.tostring(hit_price, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)

if short_sl_hit
    alert('{"signal":"UT Short SL","ticker":"' + _t + '","close":"' + _c + '","hit_price":"' + str.tostring(hit_price, "#.##") + '","interval":"' + _tf + '","exchange":"' + _ex + '"}', alert.freq_once_per_bar_close)
