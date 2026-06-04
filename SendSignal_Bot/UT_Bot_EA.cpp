//+------------------------------------------------------------------+
//|  UT_Bot_EA.mq5                                                   |
//|  Rich Foundation                                                 |
//|  1 lệnh | SL: BE → TP1 → TP2 → Trailing Stop vô hạn           |
//+------------------------------------------------------------------+
#property copyright "Rich Foundation"
#property version   "2.20"
#property description "UT Bot EA – 1 lệnh, trailing SL tự động theo 4 giai đoạn, TP vô hạn"

#include <Trade\Trade.mqh>

//=== INPUTS ================================================================
input group "=== UT Bot ==="
input int    InpKeyValue  = 1;      // Key Value (độ nhạy ATR)
input int    InpATRPeriod = 10;     // ATR Period

input group "=== TP / SL (đơn vị giá) ==="
input double InpSL        = 8.0;   // Stop Loss ban đầu
input double InpTP1       = 4.0;    // TP1 → dời SL về Breakeven
input double InpTP2       = 9.0;   // TP2 → dời SL về TP1
input double InpTP3       = 15.0;   // TP3 → TP ban đầu
input double InpTrailDist = 5.0;    // Khoảng cách Trailing Stop
input bool   InpHardTP3   = false;  // true = đóng lệnh tại TP3 | false = trailing vô hạn

input group "=== Quản lý lệnh ==="
input double InpLot       = 0.1;    // Lot Size
input int    InpMagic     = 202501; // Magic Number
input int    InpSlippage  = 30;     // Slippage (điểm trượt giá tối đa)

input group "=== Panel ==="
input int    InpPanelX    = 8;      // Vị trí X panel
input int    InpPanelY    = 20;     // Vị trí Y panel
input int    InpPanelW    = 226;    // Chiều rộng panel
input int    InpPanelH    = 320;    // Chiều cao panel

//=== STAGE =================================================================
//  NONE → OPEN (chờ TP2) → TP2_HIT (SL tại TP1, chờ Mid) → TRAIL
enum TradeStage { STAGE_NONE, STAGE_OPEN, STAGE_TP2, STAGE_TRAIL };

//=== GLOBALS ===============================================================
CTrade     g_trade;
int        g_h_atr    = INVALID_HANDLE;
double     g_ats      = 0.0;
datetime   g_last_bar = 0;

TradeStage g_stage    = STAGE_NONE;
ulong      g_ticket   = 0;
bool       g_is_long  = false;

double     g_entry = 0.0;
double     g_sl    = 0.0;
double     g_tp1   = 0.0;
double     g_tp2   = 0.0;
double     g_tp3   = 0.0;
double     g_mid   = 0.0;   // midpoint TP2–TP3, kích hoạt trailing

const string GUI = "UTB_";

//=== INIT ==================================================================
int OnInit()
{
   g_h_atr = iATR(_Symbol, PERIOD_CURRENT, InpATRPeriod);
   if(g_h_atr == INVALID_HANDLE) { Alert("UT Bot EA: Lỗi tạo ATR handle!"); return INIT_FAILED; }

   g_trade.SetExpertMagicNumber(InpMagic);
   g_trade.SetDeviationInPoints(InpSlippage);

   WarmupATS(1000);
   EventSetTimer(1);
   return INIT_SUCCEEDED;
}

//=== DEINIT ================================================================
void OnDeinit(const int reason)
{
   EventKillTimer();
   RemoveGUI();
   if(g_h_atr != INVALID_HANDLE) IndicatorRelease(g_h_atr);
}

//=== TICK ==================================================================
void OnTick()
{
   if(g_stage != STAGE_NONE)
   {
      if(!PositionSelectByTicket(g_ticket))
      {
         ResetState();
         return;
      }
      ManageTrade();
   }

   datetime t0 = iTime(_Symbol, PERIOD_CURRENT, 0);
   if(t0 == g_last_bar) return;
   g_last_bar = t0;

   ProcessBar();
}

//=== TIMER (cập nhật panel mỗi giây) ======================================
void OnTimer()
{
   UpdateGUI();
}

//=== CHART EVENT (nút Close) ===============================================
void OnChartEvent(const int id, const long& lp, const double& dp, const string& sp)
{
   if(id != CHARTEVENT_OBJECT_CLICK) return;
   if(sp == GUI + "BtnClose")
   {
      if(g_ticket != 0 && PositionSelectByTicket(g_ticket))
         g_trade.PositionClose(g_ticket);
      ResetState();
      ObjectSetInteger(0, sp, OBJPROP_STATE, false);
      ChartRedraw(0);
   }
}

//=== QUẢN LÝ SL THEO TỪNG GIAI ĐOẠN ========================================
//  OPEN  : chờ TP2 (SL giữ nguyên, bỏ qua TP1)
//  TP2   : giống code gốc — chờ Mid → SL về TP2 → trailing
//  TRAIL : trailing vô hạn
void ManageTrade()
{
   double price = g_is_long
                  ? SymbolInfoDouble(_Symbol, SYMBOL_BID)
                  : SymbolInfoDouble(_Symbol, SYMBOL_ASK);

   if(g_is_long)
   {
      switch(g_stage)
      {
         case STAGE_OPEN:
            // Ưu tiên check TP2 trước
            if(price >= g_tp2 && MoveSL(g_tp1))
               g_stage = STAGE_TP2;
            else
            {
               // Trailing với khoảng cách = InpSL (giống khoảng cách entry→SL ban đầu)
               double new_sl = NormalizeDouble(price - InpSL, _Digits);
               if(new_sl > g_sl) MoveSL(new_sl);
            }
            break;

         case STAGE_TP2:
            if(price >= g_mid && MoveSL(g_tp2))
               g_stage = STAGE_TRAIL;
            break;

         case STAGE_TRAIL:
         {
            double new_sl = NormalizeDouble(price - InpTrailDist, _Digits);
            if(new_sl > g_sl) MoveSL(new_sl);
            break;
         }

         default: break;
      }
   }
   else
   {
      switch(g_stage)
      {
         case STAGE_OPEN:
            if(price <= g_tp2 && MoveSL(g_tp1))
               g_stage = STAGE_TP2;
            else
            {
               double new_sl = NormalizeDouble(price + InpSL, _Digits);
               if(new_sl < g_sl) MoveSL(new_sl);
            }
            break;

         case STAGE_TP2:
            if(price <= g_mid && MoveSL(g_tp2))
               g_stage = STAGE_TRAIL;
            break;

         case STAGE_TRAIL:
         {
            double new_sl = NormalizeDouble(price + InpTrailDist, _Digits);
            if(new_sl < g_sl) MoveSL(new_sl);
            break;
         }

         default: break;
      }
   }
}

//=== XỬ LÝ TÍN HIỆU ========================================================
void ProcessBar()
{
   double atr_buf[];
   ArraySetAsSeries(atr_buf, true);
   if(CopyBuffer(g_h_atr, 0, 1, 1, atr_buf) < 1) return;

   double src      = iClose(_Symbol, PERIOD_CURRENT, 1);
   double src_prev = iClose(_Symbol, PERIOD_CURRENT, 2);
   double nLoss    = InpKeyValue * atr_buf[0];
   double ats_prev = g_ats;

   if     (src > ats_prev && src_prev > ats_prev) g_ats = MathMax(ats_prev, src - nLoss);
   else if(src < ats_prev && src_prev < ats_prev) g_ats = MathMin(ats_prev, src + nLoss);
   else if(src > ats_prev)                         g_ats = src - nLoss;
   else                                            g_ats = src + nLoss;

   bool buy_signal  = (src > g_ats) && (src_prev <= ats_prev);
   bool sell_signal = (src < g_ats) && (src_prev >= ats_prev);

   if(!buy_signal && !sell_signal) return;

   if(g_stage == STAGE_OPEN) return;

   if(g_stage != STAGE_NONE)
   {
      if(PositionSelectByTicket(g_ticket))
         g_trade.PositionClose(g_ticket);
      ResetState();
   }

   if     (buy_signal)  OpenLong();
   else if(sell_signal) OpenShort();
}

//=== MỞ LỆNH LONG ===========================================================
void OpenLong()
{
   double ask      = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double lot      = NormLot(InpLot);
   double sl       = NormalizeDouble(ask - InpSL, _Digits);
   double tp_order = InpHardTP3 ? NormalizeDouble(ask + InpTP3, _Digits) : 0.0;

   if(!CheckSLDist(ask, sl, true)) return;
   if(!g_trade.Buy(lot, _Symbol, ask, sl, tp_order, "UT Long")) return;

   g_ticket  = g_trade.ResultOrder();
   g_is_long = true;
   g_stage   = STAGE_OPEN;
   g_entry   = ask;
   g_sl      = sl;
   g_tp1     = NormalizeDouble(ask + InpTP1, _Digits);
   g_tp2     = NormalizeDouble(ask + InpTP2, _Digits);
   g_tp3     = NormalizeDouble(ask + InpTP3, _Digits);
   g_mid     = NormalizeDouble(ask + (InpTP2 + InpTP3) / 2.0, _Digits);
}

//=== MỞ LỆNH SHORT ==========================================================
void OpenShort()
{
   double bid      = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double lot      = NormLot(InpLot);
   double sl       = NormalizeDouble(bid + InpSL, _Digits);
   double tp_order = InpHardTP3 ? NormalizeDouble(bid - InpTP3, _Digits) : 0.0;

   if(!CheckSLDist(bid, sl, false)) return;
   if(!g_trade.Sell(lot, _Symbol, bid, sl, tp_order, "UT Short")) return;

   g_ticket  = g_trade.ResultOrder();
   g_is_long = false;
   g_stage   = STAGE_OPEN;
   g_entry   = bid;
   g_sl      = sl;
   g_tp1     = NormalizeDouble(bid - InpTP1, _Digits);
   g_tp2     = NormalizeDouble(bid - InpTP2, _Digits);
   g_tp3     = NormalizeDouble(bid - InpTP3, _Digits);
   g_mid     = NormalizeDouble(bid - (InpTP2 + InpTP3) / 2.0, _Digits);
}

//=== DỜI SL =================================================================
bool MoveSL(double new_sl)
{
   new_sl = NormalizeDouble(new_sl, _Digits);
   if(!PositionSelectByTicket(g_ticket)) return false;
   double tp = PositionGetDouble(POSITION_TP);
   if(!g_trade.PositionModify(g_ticket, new_sl, tp)) return false;
   g_sl = new_sl;
   return true;
}

//=== RESET ==================================================================
void ResetState()
{
   g_stage  = STAGE_NONE;
   g_ticket = 0;
   g_entry = g_sl = g_tp1 = g_tp2 = g_tp3 = g_mid = 0.0;
}

//=== NORMALIZE LOT ==========================================================
double NormLot(double lot)
{
   double mn   = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double mx   = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   lot = MathMax(MathMin(lot, mx), mn);
   return NormalizeDouble(MathRound(lot / step) * step, 2);
}

//=== KIỂM TRA KHOẢNG CÁCH SL ================================================
bool CheckSLDist(double price, double sl, bool is_buy)
{
   double min_d = SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL) * _Point;
   double sl_d  = is_buy ? (price - sl) : (sl - price);
   return sl_d >= min_d;
}

//=== WARM-UP ATS ============================================================
void WarmupATS(int bars)
{
   int need = bars + 5;
   double atr[], cls[];
   ArraySetAsSeries(atr, true);
   ArraySetAsSeries(cls, true);
   if(CopyBuffer(g_h_atr, 0, 0, need, atr) < need ||
      CopyClose(_Symbol, PERIOD_CURRENT, 0, need, cls) < need) return;

   double ats = 0.0;
   for(int i = bars; i >= 1; i--)
   {
      double s=cls[i], sp=cls[i+1], nl=InpKeyValue*atr[i];
      if     (s>ats&&sp>ats) ats=MathMax(ats,s-nl);
      else if(s<ats&&sp<ats) ats=MathMin(ats,s+nl);
      else if(s>ats)          ats=s-nl;
      else                    ats=s+nl;
   }
   g_ats = ats;
}

//=== GUI HELPERS =============================================================
void Lbl(string name, string text, int x, int y, color clr = clrSilver, int sz = 9)
{
   string obj = GUI + name;
   if(ObjectFind(0, obj) < 0)
   {
      ObjectCreate(0, obj, OBJ_LABEL, 0, 0, 0);
      ObjectSetInteger(0, obj, OBJPROP_CORNER,     CORNER_LEFT_UPPER);
      ObjectSetInteger(0, obj, OBJPROP_XDISTANCE,  x);
      ObjectSetInteger(0, obj, OBJPROP_YDISTANCE,  y);
      ObjectSetString(0,  obj, OBJPROP_FONT,       "Consolas");
      ObjectSetInteger(0, obj, OBJPROP_BACK,        false);
      ObjectSetInteger(0, obj, OBJPROP_SELECTABLE,  false);
   }
   ObjectSetString(0,  obj, OBJPROP_TEXT,      text);
   ObjectSetInteger(0, obj, OBJPROP_COLOR,     clr);
   ObjectSetInteger(0, obj, OBJPROP_FONTSIZE,  sz);
}

void CreateBtn(string name, string text, int x, int y, int w, int h, color bg, color border = clrSilver)
{
   string obj = GUI + name;
   if(ObjectFind(0, obj) < 0)
   {
      ObjectCreate(0, obj, OBJ_BUTTON, 0, 0, 0);
      ObjectSetInteger(0, obj, OBJPROP_CORNER,     CORNER_LEFT_UPPER);
      ObjectSetInteger(0, obj, OBJPROP_XDISTANCE,  x);
      ObjectSetInteger(0, obj, OBJPROP_YDISTANCE,  y);
      ObjectSetInteger(0, obj, OBJPROP_XSIZE,      w);
      ObjectSetInteger(0, obj, OBJPROP_YSIZE,      h);
      ObjectSetString(0,  obj, OBJPROP_FONT,       "Consolas");
      ObjectSetInteger(0, obj, OBJPROP_FONTSIZE,   9);
      ObjectSetInteger(0, obj, OBJPROP_BACK,        false);
      ObjectSetInteger(0, obj, OBJPROP_SELECTABLE,  false);
   }
   ObjectSetString(0,  obj, OBJPROP_TEXT,         text);
   ObjectSetInteger(0, obj, OBJPROP_COLOR,        clrWhite);
   ObjectSetInteger(0, obj, OBJPROP_BGCOLOR,      bg);
   ObjectSetInteger(0, obj, OBJPROP_BORDER_COLOR, border);
   ObjectSetInteger(0, obj, OBJPROP_STATE,        false);
}

void Rect(string name, int x, int y, int w, int h, color bg, color border)
{
   string obj = GUI + name;
   if(ObjectFind(0, obj) >= 0) return;
   ObjectCreate(0, obj, OBJ_RECTANGLE_LABEL, 0, 0, 0);
   ObjectSetInteger(0, obj, OBJPROP_CORNER,      CORNER_LEFT_UPPER);
   ObjectSetInteger(0, obj, OBJPROP_XDISTANCE,   x);
   ObjectSetInteger(0, obj, OBJPROP_YDISTANCE,   y);
   ObjectSetInteger(0, obj, OBJPROP_XSIZE,       w);
   ObjectSetInteger(0, obj, OBJPROP_YSIZE,       h);
   ObjectSetInteger(0, obj, OBJPROP_BGCOLOR,     bg);
   ObjectSetInteger(0, obj, OBJPROP_BORDER_TYPE, BORDER_FLAT);
   ObjectSetInteger(0, obj, OBJPROP_COLOR,       border);
   ObjectSetInteger(0, obj, OBJPROP_WIDTH,       1);
   ObjectSetInteger(0, obj, OBJPROP_BACK,        false);
   ObjectSetInteger(0, obj, OBJPROP_SELECTABLE,  false);
}

void RemoveGUI() { ObjectsDeleteAll(0, GUI); }

//=== UPDATE PANEL ============================================================
//
//  Layout (x=InpPanelX, chiều rộng 220px):
//
//  ┌─────────────────────────────┐
//  │  UT BOT EA  v2.20           │  header
//  │ ───────────────────────     │
//  │ Time  : 09:30:45            │
//  │ ATS   : 3285.40             │
//  │ ───────────────────────     │
//  │ Stage : TRAILING    [cyan]  │
//  │ Direct: BUY ▲              │
//  │ ───────────────────────     │
//  │ Entry : 3300.00             │
//  │ SL    : 3310.00             │
//  │ TP1   : 3305.00  ✅         │
//  │ TP2   : 3310.00  ✅         │
//  │ Mid   : 3312.50  ✅         │
//  │ TP3   : 3315.00 / ∞        │
//  │ ───────────────────────     │
//  │ Price : 3318.50             │
//  │ Float : +$125.00            │
//  │ ───────────────────────     │
//  │ [      Close Lệnh       ]   │
//  └─────────────────────────────┘
//
//=============================================================================
void UpdateGUI()
{
   int px = InpPanelX;
   int py = InpPanelY;
   int pw = InpPanelW;
   int lh = 15;   // line height
   int tx = px + 8;

   // ── Background ──
   Rect("BG", px, py, pw, InpPanelH, C'14,17,26', C'50,80,160');

   // ── Header ──
   int y = py + 7;
   Lbl("H",   "  UT BOT EA  v2.20",         tx, y, C'80,160,255', 10); y += lh + 4;
   Lbl("L0",  "───────────────────────",     tx, y, C'40,55,100'  );    y += lh - 2;

   // ── Thời gian & ATS ──
   MqlDateTime dt;
   TimeToStruct(TimeLocal(), dt);
   string tStr = StringFormat("%02d:%02d:%02d", dt.hour, dt.min, dt.sec);
   Lbl("Tim", "Time   : " + tStr,            tx, y, clrSilver);         y += lh;
   Lbl("Sym", "Symbol : " + _Symbol + "  " + EnumToString(Period()),
                                              tx, y, clrSilver);         y += lh;
   Lbl("ATS", StringFormat("ATS    : %s", DoubleToString(g_ats, _Digits)),
                                              tx, y, clrYellow);         y += lh;
   Lbl("L1",  "───────────────────────",     tx, y, C'40,55,100'  );    y += lh - 2;

   // ── Stage ──
   string stageTxt;
   color  stageClr;
   switch(g_stage)
   {
      case STAGE_NONE:  stageTxt = "—  Chờ tín hiệu";  stageClr = clrSilver;     break;
      case STAGE_OPEN:  stageTxt = "OPEN  (chờ TP2)";   stageClr = clrYellow;     break;
      case STAGE_TP2:   stageTxt = "TP2 HIT (chờ Mid)"; stageClr = clrLimeGreen;  break;
      case STAGE_TRAIL: stageTxt = "TRAILING  ∞";       stageClr = C'80,220,255'; break;
      default:          stageTxt = "?";                  stageClr = clrSilver;     break;
   }
   Lbl("St",  "Stage  : " + stageTxt,        tx, y, stageClr);          y += lh;

   string dirTxt = (g_stage == STAGE_NONE) ? "—" : (g_is_long ? "BUY  ▲" : "SELL ▼");
   color  dirClr = (g_stage == STAGE_NONE) ? clrSilver : (g_is_long ? clrLimeGreen : clrTomato);
   Lbl("Di",  "Direct : " + dirTxt,          tx, y, dirClr);             y += lh;
   Lbl("L2",  "───────────────────────",     tx, y, C'40,55,100'  );    y += lh - 2;

   // ── Giá các mức ──
   bool hasPos = (g_stage != STAGE_NONE);
   string na = "—";
   int d = _Digits;

   Lbl("En",  "Entry  : " + (hasPos ? DoubleToString(g_entry, d) : na),
                                              tx, y, clrSilver);         y += lh;
   Lbl("SL",  "SL     : " + (hasPos ? DoubleToString(g_sl, d) : na),
                                              tx, y, clrTomato);         y += lh;

   bool tp2done = (g_stage == STAGE_TP2 || g_stage == STAGE_TRAIL);
   bool middone = (g_stage == STAGE_TRAIL);

   Lbl("T1",  "TP1    : " + (hasPos ? DoubleToString(g_tp1, d) : na),
                                              tx, y, clrSilver);          y += lh;
   Lbl("T2",  "TP2    : " + (hasPos ? DoubleToString(g_tp2, d) : na) + (tp2done ? "  ✅" : ""),
                                              tx, y, tp2done ? clrLimeGreen : clrSilver); y += lh;
   Lbl("Mi",  "Mid    : " + (hasPos ? DoubleToString(g_mid, d) : na) + (middone ? "  ✅" : ""),
                                              tx, y, middone ? C'80,220,255' : clrSilver); y += lh;

   string tp3Str = hasPos ? (InpHardTP3 ? DoubleToString(g_tp3, d) : DoubleToString(g_tp3, d) + " / ∞") : na;
   Lbl("T3",  "TP3    : " + tp3Str,          tx, y, clrSilver);          y += lh;
   Lbl("L3",  "───────────────────────",     tx, y, C'40,55,100'  );    y += lh - 2;

   // ── Giá hiện tại & P/L ──
   double curPrice = hasPos
                     ? (g_is_long ? SymbolInfoDouble(_Symbol, SYMBOL_BID)
                                  : SymbolInfoDouble(_Symbol, SYMBOL_ASK))
                     : SymbolInfoDouble(_Symbol, SYMBOL_BID);
   Lbl("Pr",  "Price  : " + DoubleToString(curPrice, d),
                                              tx, y, clrSilver);         y += lh;

   double floatPL = 0;
   if(hasPos && PositionSelectByTicket(g_ticket))
      floatPL = PositionGetDouble(POSITION_PROFIT) + PositionGetDouble(POSITION_SWAP);

   color plClr = (floatPL >= 0) ? clrLimeGreen : clrTomato;
   string plSign = (floatPL >= 0) ? "+" : "";
   Lbl("PL",  "Float  : " + plSign + DoubleToString(floatPL, 2) + " $",
                                              tx, y, plClr);             y += lh;
   Lbl("L4",  "───────────────────────",     tx, y, C'40,55,100'  );    y += lh - 2;

   // ── Nút đóng lệnh ──
   color btnBg = hasPos ? C'20,80,170' : C'30,35,50';
   color btnBd = hasPos ? C'70,140,255' : C'50,55,70';
   CreateBtn("BtnClose", hasPos ? "✕  Close Lệnh" : "— Không có lệnh",
             px + 8, y + 3, MathMax(pw - 16, 60), 22, btnBg, btnBd);

   ChartRedraw(0);
}
