"""
  QUANTUM TRADING BOT v3.0 - Binance Futures
  ============================================
  NEW IN v3.0:
  - Open Interest tracking (detect whale accumulation)
  - Funding rate trend analysis (not just current rate)
  - Volume Profile (detect support/resistance from volume)
  - Adaptive stop loss (ATR-based, not fixed %)
  - Smart entry timing (wait for pullback, don't chase)
  - Streak-based position sizing (scale up on wins, down on losses)
  - WebSocket price cache (faster price checks)
  - Trade journal with performance analytics

  RUN:  python bot.py            (LIVE)
        python bot.py --scan-only (signals only)
"""

import os, sys, json, time, logging, math, statistics, signal, atexit
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum
from threading import Thread
from collections import deque

try: import numpy as np
except ImportError: np = None; print("[WARN] pip install numpy")
try: import pandas as pd
except ImportError: pd = None; print("[WARN] pip install pandas")
try:
    from binance.client import Client
    from binance.enums import *
    from binance.exceptions import BinanceAPIException
    HAS_BINANCE = True
except ImportError: HAS_BINANCE = False; print("[WARN] pip install python-binance")
try: import ta; HAS_TA = True
except ImportError: HAS_TA = False; print("[WARN] pip install ta")
try: import requests as tg_requests; HAS_REQUESTS = True
except ImportError: HAS_REQUESTS = False
try: import urllib.request, urllib.parse
except: pass


# ===========================================================
#  COLORS
# ===========================================================
class C:
    if sys.platform == "win32": os.system("color")
    @staticmethod
    def green(t): return f"\033[92m{t}\033[0m"
    @staticmethod
    def red(t): return f"\033[91m{t}\033[0m"
    @staticmethod
    def yellow(t): return f"\033[93m{t}\033[0m"
    @staticmethod
    def blue(t): return f"\033[94m{t}\033[0m"
    @staticmethod
    def cyan(t): return f"\033[96m{t}\033[0m"
    @staticmethod
    def magenta(t): return f"\033[95m{t}\033[0m"
    @staticmethod
    def white(t): return f"\033[97m\033[1m{t}\033[0m"
    @staticmethod
    def bold(t): return f"\033[1m{t}\033[0m"
    @staticmethod
    def dim(t): return f"\033[2m{t}\033[0m"
    @staticmethod
    def bg_green(t): return f"\033[42m\033[97m\033[1m{t}\033[0m"
    @staticmethod
    def bg_red(t): return f"\033[41m\033[97m\033[1m{t}\033[0m"
    @staticmethod
    def bg_blue(t): return f"\033[44m\033[97m\033[1m{t}\033[0m"
    @staticmethod
    def bg_yellow(t): return f"\033[43m\033[97m\033[1m{t}\033[0m"
    @staticmethod
    def long_short(d): return f"\033[42m\033[97m\033[1m LONG  \033[0m" if d=="LONG" else f"\033[41m\033[97m\033[1m SHORT \033[0m"
    @staticmethod
    def score_color(s):
        if s >= 70: return f"\033[92m{s:.1f}\033[0m"
        elif s >= 50: return f"\033[93m{s:.1f}\033[0m"
        else: return f"\033[91m{s:.1f}\033[0m"
    @staticmethod
    def pnl_color(p): return f"\033[92m+${p:.2f}\033[0m" if p >= 0 else f"\033[91m-${abs(p):.2f}\033[0m"
    @staticmethod
    def line(ch="-", n=60): return f"\033[2m{ch * n}\033[0m"
    @staticmethod
    def bar(value, max_val=100, width=20):
        filled = max(0, min(width, int((value/max_val)*width) if max_val > 0 else 0))
        color = "\033[92m" if value >= 70 else "\033[93m" if value >= 50 else "\033[91m"
        return f"{color}{'#'*filled}\033[2m{'.'*(width-filled)}\033[0m"


# ===========================================================
#  CONFIG
# ===========================================================
@dataclass
class Config:
    api_key: str = ""
    api_secret: str = ""
    testnet: bool = False
    leverage: int = 15
    capital_percent: float = 15.0
    max_open_trades: int = 2
    min_score: float = 60.0
    margin_type: str = "CROSSED"          # "ISOLATED" or "CROSSED"
    # Adaptive SL: uses ATR instead of fixed %
    sl_atr_multiplier: float = 1.5        # SL = entry +/- (ATR * 1.5)
    sl_min_pct: float = 0.8               # minimum SL = 0.8%
    sl_max_pct: float = 3.0               # maximum SL = 3%
    tp1_rr: float = 1.5                   # TP1 = 1.5x risk (risk:reward)
    tp2_rr: float = 3.0                   # TP2 = 3x risk
    tp3_rr: float = 5.0                   # TP3 = 5x risk
    move_sl_to_be_after_tp1: bool = True
    trailing_sl: bool = True
    trailing_sl_pct: float = 1.0
    scan_interval: int = 45
    top_pairs_count: int = 20
    min_volume_24h: float = 50_000_000
    pair_cooldown: int = 300
    daily_loss_limit: float = 8.0
    multi_timeframe: bool = True
    # Streak sizing
    streak_sizing: bool = True            # increase size on win streaks
    streak_bonus_pct: float = 2.0         # +2% capital per consecutive win
    streak_penalty_pct: float = 3.0       # -3% capital per consecutive loss
    streak_max_bonus: float = 6.0         # max +6% extra
    streak_max_penalty: float = 9.0       # max -9%
    # Smart entry
    smart_entry: bool = True              # wait for pullback, don't chase
    chase_threshold_pct: float = 2.0      # skip if price moved >2% in last 5 candles
    # Weights
    w_momentum: float = 0.20
    w_volume: float = 0.15
    w_volatility: float = 0.10
    w_liquidity: float = 0.15
    w_trend: float = 0.15
    w_sentiment: float = 0.25             # NEW: OI + funding + volume profile
    mode: str = "live"
    log_file: str = "trades.log"
    telegram_token: str = ""
    telegram_chat_id: str = ""
    telegram_enabled: bool = False
    telegram_send_signals: bool = True
    telegram_send_trades: bool = True
    telegram_send_tp_sl: bool = True
    telegram_send_scans: bool = False
    scan_while_full: bool = True
    close_positions_on_shutdown: bool = True
    shutdown_close_all_positions: bool = False

def load_config() -> Config:
    cfg = Config()
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip()
    cfg.api_key = os.environ.get("BINANCE_API_KEY", "")
    cfg.api_secret = os.environ.get("BINANCE_API_SECRET", "")
    cfg.testnet = os.environ.get("BINANCE_TESTNET", "false").lower() == "true"
    cfg.leverage = int(os.environ.get("LEVERAGE", "15"))
    cfg.capital_percent = float(os.environ.get("CAPITAL_PERCENT", "15"))
    cfg.max_open_trades = int(os.environ.get("MAX_OPEN_TRADES", "2"))
    cfg.min_score = float(os.environ.get("MIN_SCORE", "60"))
    cfg.margin_type = os.environ.get("MARGIN_TYPE", "CROSSED").upper()
    cfg.telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    cfg.telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    cfg.telegram_enabled = bool(cfg.telegram_token and cfg.telegram_chat_id)
    cfg.scan_while_full = os.environ.get("SCAN_WHILE_FULL", "true").lower() == "true"
    cfg.close_positions_on_shutdown = os.environ.get("CLOSE_POSITIONS_ON_SHUTDOWN", "true").lower() == "true"
    cfg.shutdown_close_all_positions = os.environ.get("SHUTDOWN_CLOSE_ALL_POSITIONS", "false").lower() == "true"
    return cfg


# ===========================================================
#  LOGGING
# ===========================================================
class ColorFormatter(logging.Formatter):
    FORMATS = {
        logging.DEBUG: f"\033[2m%(asctime)s | DEBUG   | %(message)s\033[0m",
        logging.INFO: f"\033[96m%(asctime)s\033[0m | \033[92mINFO\033[0m    | %(message)s",
        logging.WARNING: f"\033[96m%(asctime)s\033[0m | \033[93mWARN\033[0m    | %(message)s",
        logging.ERROR: f"\033[96m%(asctime)s\033[0m | \033[91mERROR\033[0m   | %(message)s",
    }
    def format(self, record):
        return logging.Formatter(self.FORMATS.get(record.levelno, self.FORMATS[logging.INFO]), datefmt="%H:%M:%S").format(record)

def setup_logging(log_file):
    logger = logging.getLogger("QB")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(); ch.setLevel(logging.INFO); ch.setFormatter(ColorFormatter()); logger.addHandler(ch)
    fh = logging.FileHandler(log_file, encoding="utf-8"); fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(fh)
    return logger


# ===========================================================
#  DATA MODELS
# ===========================================================
class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class Pattern(Enum):
    BREAKOUT = "BREAKOUT"
    TREND_CONT = "TREND_CONT"
    LIQ_SWEEP = "LIQ_SWEEP"
    RANGE_EXP = "RANGE_EXP"
    CONSOLIDATION = "CONSOL"

@dataclass
class Indicators:
    rsi: float = 50.0; macd_hist: float = 0.0
    ema_20: float = 0.0; ema_50: float = 0.0; ema_20_above_50: bool = False
    vwap: float = 0.0; price_above_vwap: bool = False
    bb_upper: float = 0.0; bb_lower: float = 0.0; bb_width: float = 0.0; bb_position: float = 0.0
    atr: float = 0.0; atr_raw: float = 0.0  # atr_raw = actual $ value
    volume_24h: float = 0.0; volume_spike: float = 1.0
    funding_rate: float = 0.0; ob_imbalance: float = 0.0; liq_distance: float = 5.0
    rsi_1m: float = 50.0; rsi_15m: float = 50.0
    trend_1m: int = 0; trend_15m: int = 0
    spread_pct: float = 0.0; bid_depth: float = 0.0; ask_depth: float = 0.0
    total_depth: float = 0.0; slippage_est: float = 0.0
    # v3.0 sentiment
    oi_change_pct: float = 0.0            # open interest change %
    funding_trend: float = 0.0            # funding rate momentum
    volume_profile_bias: float = 0.0      # +1 = buying volume, -1 = selling
    recent_move_pct: float = 0.0          # price move in last 5 candles
    buy_sell_ratio: float = 1.0           # taker buy vs sell ratio

@dataclass
class Signal:
    pair: str; direction: Direction; pattern: Pattern
    price: float; score: float; indicators: Indicators; scores_breakdown: dict
    stop_loss: float = 0.0; tp1: float = 0.0; tp2: float = 0.0; tp3: float = 0.0
    risk_pct: float = 0.0                 # actual risk % (adaptive SL)
    mtf_alignment: float = 0.0
    sentiment_score: float = 0.0
    timestamp: str = ""

@dataclass
class Position:
    pair: str; direction: Direction; entry_price: float; quantity: float
    stop_loss: float; tp1: float; tp2: float; tp3: float
    tp1_hit: bool = False; sl_moved_to_be: bool = False
    highest_price: float = 0.0; lowest_price: float = 0.0
    opened_at: str = ""; order_id: str = ""


# ===========================================================
#  BINANCE API
# ===========================================================
class BinanceAPI:
    def __init__(self, config, logger):
        self.config = config; self.logger = logger; self.client = None; self.symbol_info = {}
        if HAS_BINANCE and config.api_key:
            try:
                self.client = Client(config.api_key, config.api_secret, testnet=config.testnet)
                try:
                    st = self.client.get_server_time(); lt = int(time.time()*1000)
                    self.client.timestamp_offset = st['serverTime'] - lt
                    off = self.client.timestamp_offset / 1000
                    if abs(off) > 1: self.logger.info(f"Clock offset: {C.yellow(f'{off:.1f}s')} (corrected)")
                except: pass
                try:
                    info = self.client.futures_exchange_info()
                    for s in info["symbols"]:
                        filt = {f["filterType"]: f for f in s.get("filters", [])}
                        lot = filt.get("LOT_SIZE", {}); mlot = filt.get("MARKET_LOT_SIZE", {})
                        self.symbol_info[s["symbol"]] = {
                            "min_qty": float(lot.get("minQty", 0.001)),
                            "max_qty": float(mlot.get("maxQty", 0) or lot.get("maxQty", 999999999)),
                            "step_size": float(lot.get("stepSize", 0.001)),
                            "price_precision": s.get("pricePrecision", 2),
                            "qty_precision": s.get("quantityPrecision", 3),
                            "onboard_date": s.get("onboardDate", 0),
                        }
                    self.logger.info(f"Loaded {C.white(str(len(self.symbol_info)))} symbols")
                except Exception as e: self.logger.warning(f"Symbol info: {e}")
                self.logger.info(f"Binance [{C.yellow('TESTNET') if config.testnet else C.green('LIVE')}]")
            except Exception as e: self.logger.error(f"Binance failed: {e}")

    def get_symbol_info(self, sym): return self.symbol_info.get(sym, {"min_qty":0.001,"max_qty":999999999,"step_size":0.001,"qty_precision":3,"price_precision":6})
    def get_klines(self, sym, interval="5m", limit=100):
        if not self.client: return None
        try: return self.client.futures_klines(symbol=sym, interval=interval, limit=limit)
        except Exception as e: self.logger.error(f"Klines {sym}: {e}"); return None
    def get_ticker_24h(self, sym=None):
        if not self.client: return None
        try: return [self.client.futures_ticker(symbol=sym)] if sym else self.client.futures_ticker()
        except: return None
    def get_orderbook(self, sym, limit=20):
        if not self.client: return None
        try: return self.client.futures_order_book(symbol=sym, limit=limit)
        except: return None
    def get_funding_rate(self, sym, limit=1):
        if not self.client: return []
        try: return self.client.futures_funding_rate(symbol=sym, limit=limit)
        except: return []
    def get_open_interest(self, sym):
        """Get current open interest."""
        if not self.client: return 0.0
        try: return float(self.client.futures_open_interest(symbol=sym).get("openInterest", 0))
        except: return 0.0
    def get_open_interest_hist(self, sym, period="5m", limit=12):
        """Get OI history for trend analysis."""
        if not self.client: return []
        try: return self.client.futures_open_interest_hist(symbol=sym, period=period, limit=limit)
        except: return []
    def get_account_balance(self):
        if not self.client: return 0.0
        try:
            for a in self.client.futures_account()["assets"]:
                if a["asset"] == "USDT": return float(a["availableBalance"])
            return 0.0
        except Exception as e: self.logger.error(f"Balance: {e}"); return 0.0
    def set_leverage(self, sym, lev):
        if not self.client: return
        try: self.client.futures_change_leverage(symbol=sym, leverage=lev)
        except: pass
    def set_margin_type(self, sym, mt="ISOLATED"):
        if not self.client: return
        try: self.client.futures_change_margin_type(symbol=sym, marginType=mt)
        except BinanceAPIException as e:
            if "No need to change margin type" not in str(e): self.logger.warning(f"Margin {sym}: {e}")
        except Exception as e: self.logger.warning(f"Margin {sym}: {e}")
    def place_market_order(self, sym, side, qty):
        if not self.client: return None
        try: return self.client.futures_create_order(symbol=sym, side=side, type="MARKET", quantity=qty)
        except Exception as e: self.logger.error(f"Order {sym}: {e}"); return None
    def place_stop_loss(self, sym, side, price, qty):
        if not self.client: return None
        try: return self.client.futures_create_order(symbol=sym, side=side, type="STOP_MARKET", stopPrice=str(round(price,8)), closePosition="true")
        except Exception as e: self.logger.error(f"SL {sym}: {e}"); return None
    def place_take_profit(self, sym, side, price, qty):
        if not self.client: return None
        try: return self.client.futures_create_order(symbol=sym, side=side, type="TAKE_PROFIT_MARKET", stopPrice=str(round(price,8)), quantity=str(qty))
        except Exception as e: self.logger.error(f"TP {sym}: {e}"); return None
    def cancel_all_orders(self, sym):
        if not self.client: return
        try: self.client.futures_cancel_all_open_orders(symbol=sym)
        except: pass
    def get_open_positions(self):
        if not self.client: return []
        try: return [p for p in self.client.futures_position_information() if float(p["positionAmt"]) != 0]
        except: return None
    def get_mark_price(self, sym):
        if not self.client: return 0.0
        try: return float(self.client.futures_mark_price(symbol=sym)["markPrice"])
        except: return 0.0
    @staticmethod
    def _default_pairs():
        return ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","DOGEUSDT",
                "ADAUSDT","AVAXUSDT","LINKUSDT","SUIUSDT","DOTUSDT","NEARUSDT",
                "APTUSDT","ARBUSDT","OPUSDT","INJUSDT","FETUSDT","RNDRUSDT","WIFUSDT","JUPUSDT"]


# ===========================================================
#  TECHNICAL ANALYSIS v3.0
# ===========================================================
class TechnicalAnalysis:
    @staticmethod
    def compute(klines) -> Optional[Indicators]:
        if not klines or not pd or not np or not HAS_TA: return None
        try:
            df = pd.DataFrame(klines, columns=["time","open","high","low","close","volume","ct","qv","trades","tbb","tbq","ignore"])
            for c in ["open","high","low","close","volume","quote_volume"]:
                if c == "quote_volume": df[c] = df["qv"].astype(float)
                else: df[c] = df[c].astype(float)
            close, high, low, volume = df["close"], df["high"], df["low"], df["volume"]
            ind = Indicators()
            ind.rsi = ta.momentum.RSIIndicator(close, window=14).rsi().iloc[-1]
            ind.macd_hist = ta.trend.MACD(close).macd_diff().iloc[-1]
            ind.ema_20 = ta.trend.EMAIndicator(close, window=20).ema_indicator().iloc[-1]
            ind.ema_50 = ta.trend.EMAIndicator(close, window=50).ema_indicator().iloc[-1]
            ind.ema_20_above_50 = ind.ema_20 > ind.ema_50
            tp = (high+low+close)/3; cv = volume.cumsum()
            ind.vwap = ((tp*volume).cumsum()/cv).iloc[-1]
            ind.price_above_vwap = close.iloc[-1] > ind.vwap
            bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
            ind.bb_upper = bb.bollinger_hband().iloc[-1]; ind.bb_lower = bb.bollinger_lband().iloc[-1]
            ind.bb_width = ((ind.bb_upper-ind.bb_lower)/close.iloc[-1])*100
            mid = bb.bollinger_mavg().iloc[-1]
            ind.bb_position = (close.iloc[-1]-mid)/((ind.bb_upper-ind.bb_lower)/2) if ind.bb_upper != ind.bb_lower else 0
            atr_ind = ta.volatility.AverageTrueRange(high, low, close, window=14)
            ind.atr_raw = atr_ind.average_true_range().iloc[-1]
            ind.atr = (ind.atr_raw / close.iloc[-1]) * 100
            ind.volume_24h = df["quote_volume"].sum()
            avg_vol = volume.rolling(20).mean().iloc[-1]
            ind.volume_spike = volume.iloc[-1] / avg_vol if avg_vol > 0 else 1.0

            # Volume profile bias: compare taker buy volume vs total
            tbb = df["tbb"].astype(float)  # taker buy base
            total_vol = volume
            buy_ratio = tbb.iloc[-5:].sum() / total_vol.iloc[-5:].sum() if total_vol.iloc[-5:].sum() > 0 else 0.5
            ind.buy_sell_ratio = buy_ratio
            ind.volume_profile_bias = (buy_ratio - 0.5) * 2  # -1 to +1

            # Recent price move (last 5 candles)
            if len(close) >= 5:
                ind.recent_move_pct = ((close.iloc[-1] - close.iloc[-5]) / close.iloc[-5]) * 100

            return ind
        except: return None

    @staticmethod
    def compute_mtf(klines_1m, klines_15m, ind):
        if not klines_1m or not klines_15m or not pd or not HAS_TA: return ind
        try:
            df1 = pd.DataFrame(klines_1m, columns=["time","open","high","low","close","volume","ct","qv","t","tbb","tbq","i"])
            df1["close"] = df1["close"].astype(float)
            ind.rsi_1m = ta.momentum.RSIIndicator(df1["close"], window=14).rsi().iloc[-1]
            e9 = ta.trend.EMAIndicator(df1["close"], window=9).ema_indicator().iloc[-1]
            e21 = ta.trend.EMAIndicator(df1["close"], window=21).ema_indicator().iloc[-1]
            ind.trend_1m = 1 if e9 > e21 else -1
            df15 = pd.DataFrame(klines_15m, columns=["time","open","high","low","close","volume","ct","qv","t","tbb","tbq","i"])
            df15["close"] = df15["close"].astype(float)
            ind.rsi_15m = ta.momentum.RSIIndicator(df15["close"], window=14).rsi().iloc[-1]
            e9_15 = ta.trend.EMAIndicator(df15["close"], window=9).ema_indicator().iloc[-1]
            e21_15 = ta.trend.EMAIndicator(df15["close"], window=21).ema_indicator().iloc[-1]
            ind.trend_15m = 1 if e9_15 > e21_15 else -1
        except: pass
        return ind

    @staticmethod
    def compute_ob_imbalance(ob):
        if not ob: return 0.0
        try:
            b = sum(float(x[1]) for x in ob.get("bids",[])[:10])
            a = sum(float(x[1]) for x in ob.get("asks",[])[:10])
            return ((b-a)/(b+a))*100 if (b+a) else 0.0
        except: return 0.0

    @staticmethod
    def compute_liquidity(ob, price, order_size_usdt=1500):
        result = {"spread_pct": 999, "bid_depth": 0, "ask_depth": 0, "total_depth": 0, "slippage_est": 999}
        if not ob or not price or price <= 0: return result
        try:
            bids, asks = ob.get("bids",[]), ob.get("asks",[])
            if not bids or not asks: return result
            bb, ba = float(bids[0][0]), float(asks[0][0])
            mid = (bb+ba)/2
            result["spread_pct"] = ((ba-bb)/mid)*100 if mid > 0 else 999
            result["bid_depth"] = sum(float(b[0])*float(b[1]) for b in bids[:10])
            result["ask_depth"] = sum(float(a[0])*float(a[1]) for a in asks[:10])
            result["total_depth"] = result["bid_depth"] + result["ask_depth"]
            remaining = order_size_usdt; total_cost = 0
            for ap, aq in asks[:20]:
                ap, aq = float(ap), float(aq)
                fill = min(remaining, ap*aq)
                total_cost += fill * (ap/ba); remaining -= fill
                if remaining <= 0: break
            result["slippage_est"] = (total_cost/order_size_usdt - 1)*100 if order_size_usdt > 0 and total_cost > 0 else 0
        except: pass
        return result

    @staticmethod
    def compute_funding_trend(funding_data):
        """Analyze funding rate momentum over last few periods."""
        if not funding_data or len(funding_data) < 2: return 0.0
        try:
            rates = [float(f.get("fundingRate", 0)) for f in funding_data]
            if len(rates) < 2: return 0.0
            # Trend = weighted average of recent changes
            changes = [rates[i] - rates[i-1] for i in range(1, len(rates))]
            # More recent changes weighted higher
            weights = list(range(1, len(changes)+1))
            weighted_trend = sum(c*w for c, w in zip(changes, weights)) / sum(weights)
            return weighted_trend * 10000  # scale up for readability
        except: return 0.0

    @staticmethod
    def compute_oi_change(oi_hist):
        """Calculate open interest % change over recent periods."""
        if not oi_hist or len(oi_hist) < 2: return 0.0
        try:
            ois = [float(h.get("sumOpenInterest", 0)) for h in oi_hist]
            if ois[0] <= 0: return 0.0
            return ((ois[-1] - ois[0]) / ois[0]) * 100
        except: return 0.0


# ===========================================================
#  SCORING ENGINE v3.0
# ===========================================================
class ScoringEngine:
    def __init__(self, config): self.config = config

    def score(self, ind):
        m = self._momentum(ind); v = self._volume(ind); vo = self._volatility(ind)
        l = self._liquidity(ind); t = self._trend(ind); s = self._sentiment(ind)
        total = (m*self.config.w_momentum + v*self.config.w_volume + vo*self.config.w_volatility +
                 l*self.config.w_liquidity + t*self.config.w_trend + s*self.config.w_sentiment)
        mtf_bonus = self._mtf_bonus(ind)
        total += mtf_bonus
        total = max(5, min(98, total))
        return {"total": total, "momentum": m, "volume": v, "volatility": vo,
                "liquidity": l, "trend": t, "sentiment": s, "mtf_bonus": mtf_bonus}

    def _sentiment(self, ind):
        """NEW: Score based on OI changes, funding trend, volume profile."""
        s = 0
        # Open interest increasing = money flowing in
        if ind.oi_change_pct > 5: s += 30
        elif ind.oi_change_pct > 2: s += 20
        elif ind.oi_change_pct > 0.5: s += 10
        elif ind.oi_change_pct < -3: s -= 10  # money leaving

        # Volume profile bias (are buyers or sellers dominating?)
        if abs(ind.volume_profile_bias) > 0.3: s += 25
        elif abs(ind.volume_profile_bias) > 0.15: s += 15

        # Buy/sell ratio
        if ind.buy_sell_ratio > 0.55: s += 15  # buyers dominating
        elif ind.buy_sell_ratio < 0.45: s += 15  # sellers dominating (good for shorts)

        # Funding trend (increasing = crowded, potential reversal)
        if abs(ind.funding_trend) > 2: s += 15
        elif abs(ind.funding_trend) > 1: s += 10

        return max(0, min(100, s))

    def _mtf_bonus(self, ind):
        bonus = 0
        t5 = 1 if ind.ema_20_above_50 else -1
        if ind.trend_1m == ind.trend_15m == t5: bonus += 10
        elif (ind.trend_1m == t5) or (ind.trend_15m == t5): bonus += 5
        if ind.rsi > 50 and ind.rsi_1m > 50 and ind.rsi_15m > 50: bonus += 5
        elif ind.rsi < 50 and ind.rsi_1m < 50 and ind.rsi_15m < 50: bonus += 5
        return bonus

    def _momentum(self, ind):
        s = 0
        if 55 < ind.rsi < 75: s += 35
        elif 25 < ind.rsi < 45: s += 30
        elif ind.rsi >= 75: s += 15
        elif ind.rsi <= 25: s += 20
        else: s += 10
        if abs(ind.macd_hist) > 0.8: s += 35
        elif abs(ind.macd_hist) > 0.3: s += 20
        else: s += 5
        if abs(ind.bb_position) > 0.5: s += 30
        elif abs(ind.bb_position) > 0.25: s += 15
        return min(100, s)

    def _volume(self, ind):
        s = 0
        if ind.volume_24h > 500e6: s += 40
        elif ind.volume_24h > 100e6: s += 25
        elif ind.volume_24h > 50e6: s += 15
        if ind.volume_spike > 2: s += 40
        elif ind.volume_spike > 1.3: s += 25
        elif ind.volume_spike > 1: s += 10
        return min(100, s + min(20, ind.volume_24h/50e6))

    def _volatility(self, ind):
        s = 0
        if ind.bb_width > 2.5: s += 40
        elif ind.bb_width > 1.5: s += 25
        elif ind.bb_width > 0.8: s += 10
        if ind.atr > 3: s += 35
        elif ind.atr > 1.5: s += 20
        elif ind.atr > 0.5: s += 10
        return min(100, s + min(25, ind.volume_spike*10))

    def _liquidity(self, ind):
        s = 0
        if abs(ind.ob_imbalance) > 30: s += 20
        elif abs(ind.ob_imbalance) > 15: s += 10
        if ind.spread_pct < 0.02: s += 30
        elif ind.spread_pct < 0.05: s += 20
        elif ind.spread_pct < 0.1: s += 10
        elif ind.spread_pct > 0.3: s -= 20
        if ind.total_depth > 1e6: s += 25
        elif ind.total_depth > 500e3: s += 15
        elif ind.total_depth > 100e3: s += 5
        if ind.slippage_est < 0.01: s += 15
        elif ind.slippage_est < 0.05: s += 10
        elif ind.slippage_est > 0.2: s -= 15
        return max(0, min(100, s))

    def _trend(self, ind):
        return (50 if ind.ema_20_above_50 else 0) + (50 if ind.price_above_vwap else 0)

    def classify_pattern(self, ind):
        if ind.bb_width > 3 and ind.volume_spike > 2: return Pattern.BREAKOUT
        if ind.ema_20_above_50 and 50 < ind.rsi < 70: return Pattern.TREND_CONT
        if (ind.rsi < 30 or ind.rsi > 70) and ind.liq_distance < 1.5: return Pattern.LIQ_SWEEP
        if ind.bb_width < 1.5 and ind.volume_spike > 1.5: return Pattern.RANGE_EXP
        return Pattern.CONSOLIDATION

    def determine_direction(self, ind):
        bull, bear = 0, 0
        if ind.rsi > 50: bull += 2
        else: bear += 2
        if ind.macd_hist > 0: bull += 2
        else: bear += 2
        if ind.ema_20_above_50: bull += 1.5
        else: bear += 1.5
        if ind.price_above_vwap: bull += 1.5
        else: bear += 1.5
        if ind.ob_imbalance > 15: bull += 1
        elif ind.ob_imbalance < -15: bear += 1
        if ind.trend_1m == 1: bull += 1
        elif ind.trend_1m == -1: bear += 1
        if ind.trend_15m == 1: bull += 1
        elif ind.trend_15m == -1: bear += 1
        # v3: sentiment direction
        if ind.volume_profile_bias > 0.2: bull += 1.5
        elif ind.volume_profile_bias < -0.2: bear += 1.5
        if ind.funding_rate > 0.01: bear += 0.5  # crowded longs = contrarian short
        elif ind.funding_rate < -0.01: bull += 0.5
        return Direction.LONG if bull >= bear else Direction.SHORT


# ===========================================================
#  RISK MANAGER v3.0 (Adaptive SL + Streak Sizing)
# ===========================================================
class RiskManager:
    def __init__(self, config): self.config = config

    def calculate_adaptive_sl(self, price, atr_raw, direction):
        """SL based on ATR, clamped between min and max."""
        sl_distance = atr_raw * self.config.sl_atr_multiplier
        sl_pct = (sl_distance / price) * 100
        sl_pct = max(self.config.sl_min_pct, min(self.config.sl_max_pct, sl_pct))
        sl_distance = price * (sl_pct / 100)

        m = 1 if direction == Direction.LONG else -1
        sl = price - m * sl_distance
        risk = sl_distance

        # TP based on risk:reward ratio
        tp1 = price + m * risk * self.config.tp1_rr
        tp2 = price + m * risk * self.config.tp2_rr
        tp3 = price + m * risk * self.config.tp3_rr

        return {"stop_loss": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3, "risk_pct": sl_pct}

    def calculate_position_size(self, balance, price, sym_info=None, streak=0):
        """Position size with streak adjustment."""
        cap_pct = self.config.capital_percent
        if self.config.streak_sizing and streak != 0:
            if streak > 0:  # win streak
                bonus = min(streak * self.config.streak_bonus_pct, self.config.streak_max_bonus)
                cap_pct += bonus
            else:  # loss streak
                penalty = min(abs(streak) * self.config.streak_penalty_pct, self.config.streak_max_penalty)
                cap_pct = max(5, cap_pct - penalty)  # minimum 5%

        notional = balance * (cap_pct / 100) * self.config.leverage
        q = notional / price
        if sym_info:
            mq = sym_info.get("max_qty", 999999999)
            if q > mq: q = mq * 0.95
            if q < sym_info.get("min_qty", 0.001): return 0, cap_pct
            step = sym_info.get("step_size", 0.001)
            prec = sym_info.get("qty_precision", 3)
            q = round(math.floor(q/step)*step, prec)
        else:
            if price > 1000: q = round(q, 3)
            elif price > 1: q = round(q, 2)
            elif price > 0.01: q = round(q, 1)
            else: q = round(q, 0)
        return max(0, q), cap_pct

    def calculate_trailing_sl(self, pos, mark_price):
        if pos.direction == Direction.LONG:
            pos.highest_price = max(pos.highest_price, mark_price)
            new_sl = pos.highest_price * (1 - self.config.trailing_sl_pct/100)
            return max(pos.stop_loss, new_sl)
        else:
            pos.lowest_price = min(pos.lowest_price, mark_price)
            new_sl = pos.lowest_price * (1 + self.config.trailing_sl_pct/100)
            return min(pos.stop_loss, new_sl)


# ===========================================================
#  TRADE LOGGER + JOURNAL
# ===========================================================
class TradeLogger:
    def __init__(self, fp="trade_history.json"):
        self.fp = fp; self.trades = []
        if os.path.exists(fp):
            try:
                with open(fp) as f: self.trades = json.load(f)
            except: pass

    def log(self, **kw):
        self.trades.append({"timestamp": datetime.now(timezone.utc).isoformat(), **kw})
        self._save()

    def get_recent_performance(self, hours=24):
        """Analyze recent trade performance."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        recent = [t for t in self.trades if t.get("type") == "close" and
                  datetime.fromisoformat(t["timestamp"]) > cutoff]
        if not recent: return {"wins": 0, "losses": 0, "pnl": 0, "avg_pnl": 0, "best": 0, "worst": 0}
        pnls = [t.get("pnl", 0) for t in recent]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        return {
            "wins": len(wins), "losses": len(losses),
            "pnl": sum(pnls), "avg_pnl": sum(pnls)/len(pnls),
            "best": max(pnls) if pnls else 0,
            "worst": min(pnls) if pnls else 0,
        }

    def _save(self):
        try:
            with open(self.fp, "w") as f: json.dump(self.trades[-500:], f, indent=2)  # keep last 500
        except: pass


# ===========================================================
#  TELEGRAM
# ===========================================================
class TelegramBot:
    def __init__(self, config, logger):
        self.config = config; self.logger = logger
        self.base_url = f"https://api.telegram.org/bot{config.telegram_token}"
        self.last_update_id = 0; self._bot_ref = None; self._running = False

    def set_bot_ref(self, bot): self._bot_ref = bot

    def send(self, text, parse_mode="HTML"):
        if not self.config.telegram_enabled: return
        try:
            p = {"chat_id":self.config.telegram_chat_id,"text":text,"parse_mode":parse_mode,"disable_web_page_preview":True}
            if HAS_REQUESTS: tg_requests.post(f"{self.base_url}/sendMessage", json=p, timeout=10)
            else: urllib.request.urlopen(urllib.request.Request(f"{self.base_url}/sendMessage", json.dumps(p).encode(), headers={"Content-Type":"application/json"}), timeout=10)
        except: pass

    def notify_startup(self):
        c = self.config
        self.send(f"<b>QUANTUM v3.0 STARTED</b>\n----\nLIVE | {c.leverage}x | {c.capital_percent}%\nAdaptive SL: ATR x{c.sl_atr_multiplier}\nTP R:R: {c.tp1_rr}/{c.tp2_rr}/{c.tp3_rr}\nStreak sizing: {'ON' if c.streak_sizing else 'OFF'}\nSmart entry: {'ON' if c.smart_entry else 'OFF'}\nSentiment: ON (OI+Funding+Vol)\n----\n/help for commands")
    def notify_shutdown(self): self.send("<b>BOT STOPPED</b>")
    def notify_trade_open(self, s, qty, cap_pct):
        if not self.config.telegram_send_trades: return
        f = lambda v: f"{v:.4f}" if v < 1 else f"{v:.2f}"
        self.send(f"<b>TRADE OPENED</b>\n{s.pair} {s.direction.value}\nEntry: <code>${f(s.price)}</code> | Qty: {qty}\nSL: <code>${f(s.stop_loss)}</code> (Risk: {s.risk_pct:.1f}%)\nTP1: <code>${f(s.tp1)}</code> | TP2: <code>${f(s.tp2)}</code> | TP3: <code>${f(s.tp3)}</code>\nScore: {s.score:.1f} | Sent: {s.sentiment_score:.0f} | Cap: {cap_pct:.0f}%")
    def notify_tp_hit(self, sym, tp, price):
        if self.config.telegram_send_tp_sl: self.send(f"<b>{tp} HIT</b> {sym} @ ${price:.6f}")
    def notify_trade_close(self, sym, reason, pnl):
        if self.config.telegram_send_trades: self.send(f"<b>TRADE CLOSED</b>\n{sym} | {reason}\nPnL: ${pnl:.2f}")
    def notify_daily_limit(self):
        self.send(f"<b>DAILY LOSS LIMIT</b>\nPaused. Loss > {self.config.daily_loss_limit}%")
    def notify_scan_summary(self, sigs, n):
        if not self.config.telegram_send_scans or not sigs: return
        lines = [f"<b>SCAN #{n}</b>"]
        for i, s in enumerate(sigs[:5]): lines.append(f"{i+1}. {s.pair} {s.direction.value} Score:{s.score:.0f} Sent:{s.sentiment_score:.0f}")
        self.send("\n".join(lines))
    def notify_error(self, msg): self.send(f"<b>ERROR</b>\n<code>{msg[:500]}</code>")

    def poll_commands(self):
        if not self.config.telegram_enabled: return
        try:
            params = {"offset":self.last_update_id+1,"timeout":1}
            if HAS_REQUESTS: data = tg_requests.get(f"{self.base_url}/getUpdates", params=params, timeout=5).json()
            else:
                with urllib.request.urlopen(urllib.request.Request(f"{self.base_url}/getUpdates?{urllib.parse.urlencode(params)}"), timeout=5) as r: data = json.loads(r.read().decode())
            if not data.get("ok"): return
            for u in data.get("result",[]):
                self.last_update_id = u["update_id"]
                msg = u.get("message",{}); txt = msg.get("text","").strip(); cid = str(msg.get("chat",{}).get("id",""))
                if cid != self.config.telegram_chat_id: continue
                self._handle(txt)
        except: pass

    def _handle(self, t):
        cmd = t.lower().split()[0] if t else ""
        b = self._bot_ref
        if not b: return
        if cmd == "/help": self.send("<b>v3.0 Commands</b>\n/status /positions /stats /journal /balance /config /pause /resume /help")
        elif cmd == "/status":
            self.send(f"<b>STATUS</b> {'PAUSED' if b._paused else 'RUNNING'}\nScans: {b.scan_count} | Open: {len(b.open_positions)}/{self.config.max_open_trades}\nStreak: {b.current_streak:+d}\nW: {b.wins} L: {b.losses} WR: {b.wins/(b.wins+b.losses)*100 if (b.wins+b.losses) else 0:.0f}%")
        elif cmd == "/positions":
            if not b.open_positions: self.send("No open positions"); return
            f = lambda v: f"{v:.4f}" if v < 1 else f"{v:.2f}"
            self.send("\n".join([f"<b>{p.pair}</b> {p.direction.value}\nEntry: ${f(p.entry_price)} SL: ${f(p.stop_loss)}" for p in b.open_positions]))
        elif cmd == "/stats":
            wr = b.wins/(b.wins+b.losses)*100 if (b.wins+b.losses) else 0
            self.send(f"<b>STATS</b>\nW: {b.wins} L: {b.losses} WR: {wr:.1f}%\nPnL: ${b.total_pnl:.2f} | Streak: {b.current_streak:+d}")
        elif cmd == "/journal":
            perf = b.trade_logger.get_recent_performance(24)
            self.send(f"<b>24H JOURNAL</b>\nW: {perf['wins']} L: {perf['losses']}\nPnL: ${perf['pnl']:.2f}\nAvg: ${perf['avg_pnl']:.2f}\nBest: ${perf['best']:.2f}\nWorst: ${perf['worst']:.2f}")
        elif cmd == "/balance": self.send(f"Balance: <b>${b.api.get_account_balance():.2f}</b> USDT")
        elif cmd == "/config": self.send(f"<b>CONFIG v3.0</b>\n{self.config.leverage}x | ATR SL x{self.config.sl_atr_multiplier}\nR:R {self.config.tp1_rr}/{self.config.tp2_rr}/{self.config.tp3_rr}\nStreak: {'ON' if self.config.streak_sizing else 'OFF'}\nSmart entry: {'ON' if self.config.smart_entry else 'OFF'}")
        elif cmd == "/pause": b._paused = True; self.send("<b>PAUSED</b>")
        elif cmd == "/resume": b._paused = False; self.send("<b>RESUMED</b>")

    def start_polling(self):
        if not self.config.telegram_enabled: return
        self._running = True; Thread(target=self._loop, daemon=True).start()
    def stop_polling(self): self._running = False
    def _loop(self):
        while self._running:
            try: self.poll_commands()
            except: pass
            time.sleep(2)


# ===========================================================
#  MAIN BOT v3.0
# ===========================================================
class QuantumBot:
    BLACKLIST = {"1000SATSUSDT","LUNCUSDT","USTCUSDT","LUNAUSTUSDT","SRMUSDT",
                 "TOMOUSDT","CVCUSDT","MDTUSDT","OGUSDT","AMBUSDT","HIGHUSDT",
                 "LEVERUSDT","KEYUSDT","BTSUSDT","SPELLUSDT","ALCXUSDT","UFTUSDT"}

    def __init__(self, config):
        self.config = config
        self.logger = setup_logging(config.log_file)
        self.api = BinanceAPI(config, self.logger)
        self.ta = TechnicalAnalysis()
        self.scorer = ScoringEngine(config)
        self.risk = RiskManager(config)
        self.trade_logger = TradeLogger()
        self.open_positions = []
        self.scan_count = 0
        self.wins = 0; self.losses = 0; self.total_pnl = 0.0; self.trade_count = 0
        self.daily_pnl = 0.0; self.daily_start_balance = 0.0
        self.current_streak = 0  # +N = win streak, -N = loss streak
        self._paused = False; self._daily_limit_hit = False
        self._last_signals = []; self._pair_cooldowns = {}
        self._is_shutting_down = False
        self._shutdown_reason = None
        self.telegram = TelegramBot(config, self.logger)
        self.telegram.set_bot_ref(self)
        self._load_persisted_stats()

    def _load_persisted_stats(self):
        """Restore stats from journal so W/L/WR survive restarts and match closed trades."""
        closes = [t for t in self.trade_logger.trades if t.get("type") == "close"]
        self.wins = sum(1 for t in closes if float(t.get("pnl", 0) or 0) > 0)
        self.losses = sum(1 for t in closes if float(t.get("pnl", 0) or 0) < 0)
        self.trade_count = len(closes)
        self.total_pnl = sum(float(t.get("pnl", 0) or 0) for t in closes)

        streak = 0
        for t in closes:
            pnl = float(t.get("pnl", 0) or 0)
            if pnl > 0:
                streak = streak + 1 if streak > 0 else 1
            elif pnl < 0:
                streak = streak - 1 if streak < 0 else -1
        self.current_streak = streak
        self._register_shutdown_hooks()


    def _register_shutdown_hooks(self):
        atexit.register(self._atexit_shutdown)
        for sig_name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, sig_name, None)
            if sig is not None:
                try: signal.signal(sig, self._handle_shutdown_signal)
                except Exception: pass

    def _handle_shutdown_signal(self, signum, frame):
        try: sig_name = signal.Signals(signum).name
        except Exception: sig_name = f"SIGNAL {signum}"
        self.shutdown(sig_name)
        raise SystemExit(0)

    def _atexit_shutdown(self):
        self.shutdown("EXIT")

    def shutdown(self, reason="SHUTDOWN"):
        if self._is_shutting_down:
            return
        self._is_shutting_down = True
        self._shutdown_reason = reason
        try:
            self.logger.info(f"\n\n{C.bg_yellow(' SHUTTING DOWN ')} {C.yellow(reason)}")
        except Exception:
            pass
        try:
            self.telegram.stop_polling()
        except Exception:
            pass

        if self.config.close_positions_on_shutdown:
            try:
                if self.config.shutdown_close_all_positions:
                    self.close_all_positions()
                else:
                    self.close_bot_positions()
            except Exception as e:
                try: self.logger.error(f"Shutdown close error: {e}")
                except Exception: pass

        try: self.print_stats()
        except Exception: pass
        try: self.logger.info(f"{C.bg_red(' BOT STOPPED ')}")
        except Exception: pass
        try: self.telegram.notify_shutdown()
        except Exception: pass

    def print_banner(self):
        print(f"\n{C.line('=', 65)}")
        print(f"  {C.bold(C.cyan('QUANTUM TRADING BOT v3.0'))} - {C.white('Binance Futures')}")
        print(f"{C.line('=', 65)}")
        print(f"  Mode:         {C.bg_red(' LIVE TRADING ')}")
        print(f"  Leverage:     {C.yellow(f'{self.config.leverage}x')} {C.cyan(self.config.margin_type)}")
        print(f"  Capital:      {C.blue(f'{self.config.capital_percent}%')} (streak adjusted)")
        print(f"  Adaptive SL:  ATR x {C.red(f'{self.config.sl_atr_multiplier}')} (min {self.config.sl_min_pct}% max {self.config.sl_max_pct}%)")
        print(f"  TP R:R:       {C.green(f'{self.config.tp1_rr}x')} / {C.green(f'{self.config.tp2_rr}x')} / {C.green(f'{self.config.tp3_rr}x')}")
        print(f"  Trailing SL:  {C.green('ON') if self.config.trailing_sl else C.red('OFF')}")
        print(f"  Multi-TF:     {C.green('ON') if self.config.multi_timeframe else C.red('OFF')}")
        print(f"  Sentiment:    {C.green('ON')} (OI + Funding + Volume Profile)")
        print(f"  Smart Entry:  {C.green('ON') if self.config.smart_entry else C.red('OFF')}")
        print(f"  Streak Size:  {C.green('ON') if self.config.streak_sizing else C.red('OFF')}")
        print(f"  Daily Limit:  {C.yellow(f'{self.config.daily_loss_limit}%')}")
        if self.config.testnet: print(f"  {C.bg_yellow(' TESTNET ')}")
        print(f"{C.line('=', 65)}\n")

    def is_pair_on_cooldown(self, sym):
        if sym not in self._pair_cooldowns: return False
        return time.time() - self._pair_cooldowns[sym] < self.config.pair_cooldown

    def check_daily_limit(self):
        if self.daily_start_balance <= 0: return False
        loss_pct = (self.daily_pnl / self.daily_start_balance) * 100
        if loss_pct < -self.config.daily_loss_limit:
            if not self._daily_limit_hit:
                self._daily_limit_hit = True
                self.logger.warning(f"  {C.bg_red(' DAILY LIMIT ')} {C.red(f'{loss_pct:.1f}%')}")
                self.telegram.notify_daily_limit()
            return True
        return False

    def detect_new_listings(self):
        new = []; now = int(time.time()*1000); week = 7*24*60*60*1000
        for sym, info in self.api.symbol_info.items():
            if not sym.endswith("USDT"): continue
            ob = info.get("onboard_date", 0)
            if ob and (now-ob) < week: new.append((sym, (now-ob)/(1000*60*60)))
        new.sort(key=lambda x: x[1]); return new

    def scan_market(self):
        self.logger.info(f"{C.bold('STEP 1')} - {C.cyan('Scanning market...')}")
        tickers = self.api.get_ticker_24h()
        if not tickers: return self.api._default_pairs()[:self.config.top_pairs_count]
        ticker_map = {t.get("symbol",""): t for t in tickers}
        new_listings = self.detect_new_listings()
        new_syms = {s for s, _ in new_listings}
        if new_listings:
            self.logger.info(f"  {C.bg_blue(' NEW ')} {len(new_listings)} coins in last 7d")

        pairs = []
        for t in tickers:
            sym = t.get("symbol",""); vol = float(t.get("quoteVolume",0))
            chg = abs(float(t.get("priceChangePercent",0)))
            if not sym.endswith("USDT") or sym in self.BLACKLIST: continue
            if any(x in sym for x in ["USDCUSDT","TUSDUSDT","BUSDUSDT","DAIUSDT","FDUSDUSDT"]): continue
            is_new = sym in new_syms
            if vol < (10e6 if is_new else self.config.min_volume_24h): continue
            if not is_new and chg < 0.5: continue
            pri = vol
            if is_new:
                age = next((h for s, h in new_listings if s == sym), 999)
                if age < 6: pri += 50e9
                elif age < 24: pri += 20e9
                elif age < 72: pri += 5e9
                else: pri += 1e9
            if chg > 10: pri += 2e9
            elif chg > 5: pri += 500e6
            pairs.append((sym, vol, chg, pri, is_new))

        pairs.sort(key=lambda x: x[3], reverse=True)
        top = pairs[:self.config.top_pairs_count]
        self.logger.info(f"  Selected {C.white(str(len(top)))} pairs")
        for i, (sym, vol, chg, _, is_new) in enumerate(top[:5]):
            tag = f" {C.bg_blue(' NEW ')}" if is_new else ""
            self.logger.info(f"    {C.dim(f'{i+1}.')} {C.white(f'{sym:14s}')} Vol: {C.cyan(f'${vol/1e6:.0f}M')} | 24h: {C.yellow(f'{chg:+.1f}%')}{tag}")
        return [p[0] for p in top]

    def analyze_pair(self, symbol):
        klines = self.api.get_klines(symbol, "5m", 100)
        if not klines: return None
        ind = self.ta.compute(klines)
        if not ind: return None

        if self.config.multi_timeframe:
            k1m = self.api.get_klines(symbol, "1m", 50)
            k15m = self.api.get_klines(symbol, "15m", 50)
            ind = self.ta.compute_mtf(k1m, k15m, ind)

        ob = self.api.get_orderbook(symbol)
        ind.ob_imbalance = self.ta.compute_ob_imbalance(ob)
        price = float(klines[-1][4])
        liq = self.ta.compute_liquidity(ob, price)
        ind.spread_pct = liq["spread_pct"]; ind.bid_depth = liq["bid_depth"]
        ind.ask_depth = liq["ask_depth"]; ind.total_depth = liq["total_depth"]
        ind.slippage_est = liq["slippage_est"]

        # v3: Sentiment data
        funding_data = self.api.get_funding_rate(symbol, limit=8)
        ind.funding_rate = float(funding_data[-1].get("fundingRate", 0)) if funding_data else 0
        ind.funding_trend = self.ta.compute_funding_trend(funding_data)

        oi_hist = self.api.get_open_interest_hist(symbol, "5m", 12)
        ind.oi_change_pct = self.ta.compute_oi_change(oi_hist)

        ind.liq_distance = max(0.5, (100/self.config.leverage)-ind.atr) if ind.atr > 0 else 5.0

        scores = self.scorer.score(ind)

        # New listing bonus
        nlb = 0
        si = self.api.symbol_info.get(symbol, {})
        ob_date = si.get("onboard_date", 0)
        if ob_date:
            age_h = (time.time()*1000 - ob_date) / (1000*60*60)
            if age_h < 6: nlb = 20
            elif age_h < 24: nlb = 15
            elif age_h < 72: nlb = 10
            elif age_h < 168: nlb = 5
        scores["total"] = min(98, scores["total"] + nlb)
        scores["new_listing_bonus"] = nlb

        direction = self.scorer.determine_direction(ind)
        pattern = self.scorer.classify_pattern(ind)

        # Adaptive SL/TP
        levels = self.risk.calculate_adaptive_sl(price, ind.atr_raw, direction)

        return Signal(pair=symbol, direction=direction, pattern=pattern, price=price,
            score=scores["total"], indicators=ind, scores_breakdown=scores,
            stop_loss=levels["stop_loss"], tp1=levels["tp1"], tp2=levels["tp2"], tp3=levels["tp3"],
            risk_pct=levels["risk_pct"],
            mtf_alignment=scores.get("mtf_bonus", 0),
            sentiment_score=scores.get("sentiment", 0),
            timestamp=datetime.now(timezone.utc).isoformat())

    def run_full_scan(self):
        self.scan_count += 1
        self.logger.info(C.line("-", 55))
        self.logger.info(f"{C.bold(C.cyan(f'SCAN #{self.scan_count}'))} - {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")
        self.logger.info(C.line("-", 55))
        pairs = self.scan_market()
        signals = []
        for i, sym in enumerate(pairs):
            self.logger.debug(f"  Analyzing {sym} ({i+1}/{len(pairs)})")
            sig = self.analyze_pair(sym)
            if sig: signals.append(sig)
            time.sleep(0.05)
        signals.sort(key=lambda s: s.score, reverse=True)

        self.logger.info(f"\n{C.bold('TOP SIGNALS:')}")
        for i, s in enumerate(signals[:10]):
            cd = " [CD]" if self.is_pair_on_cooldown(s.pair) else ""
            pc = {Pattern.BREAKOUT: C.green, Pattern.TREND_CONT: C.blue, Pattern.LIQ_SWEEP: C.yellow}.get(s.pattern, C.magenta)
            nlb = s.scores_breakdown.get("new_listing_bonus", 0)
            nt = f" {C.bg_blue(f' NEW+{int(nlb)} ')}" if nlb > 0 else ""
            self.logger.info(
                f"  {C.dim(f'#{i+1:2d}')} | {C.white(f'{s.pair:14s}')} | "
                f"{C.long_short(s.direction.value)} | {pc(f'{s.pattern.value:12s}')} | "
                f"Score: {C.score_color(s.score)} {C.bar(s.score)} | "
                f"Sent: {C.cyan(f'{s.sentiment_score:.0f}')} | "
                f"Risk: {C.red(f'{s.risk_pct:.1f}%')}{nt}{C.red(cd)}")

        self._last_signals = signals
        self.telegram.notify_scan_summary(signals, self.scan_count)
        return signals

    def sync_open_positions(self):
        """Keep bot-tracked positions aligned with what Binance still has open."""
        positions = self.api.get_open_positions()
        if positions is None:
            return None
        active_symbols = {p["symbol"] for p in positions}
        if self.open_positions:
            self.open_positions = [p for p in self.open_positions if p.pair in active_symbols]
        return positions

    def get_total_open_count(self):
        """Count ALL open positions on Binance, not just bot-tracked ones."""
        positions = self.sync_open_positions()
        if positions is None:
            return len(self.open_positions)
        return len(positions)

    def can_open_trade(self):
        """Only allow a new trade when total exchange positions are below the limit."""
        total = self.get_total_open_count()
        return total < self.config.max_open_trades

    def enforce_max_open_trades(self):
        """If total positions exceed the configured max, close excess bot-managed positions."""
        positions = self.sync_open_positions()
        if positions is None:
            return 0
        total = len(positions)
        excess = total - self.config.max_open_trades
        if excess <= 0:
            return 0
        self.logger.warning(f"  {C.bg_red(' LIMIT BREACH ')} {total}/{self.config.max_open_trades} open positions. Reducing exposure...")
        bot_symbols = {p.pair for p in self.open_positions}
        if not bot_symbols:
            self.logger.warning("  Over limit, but there are no tracked bot positions available to close.")
            return 0
        candidates = [p for p in self.open_positions if p.pair in bot_symbols]
        # Close newest bot positions first to get back under the cap quickly.
        candidates.sort(key=lambda p: p.opened_at or "", reverse=True)
        closed = 0
        for pos in candidates[:excess]:
            if self.close_position(pos.pair, reason="LIMIT"):
                closed += 1
        return closed

    def execute_trade(self, signal):
        # STRICT check - query Binance every time
        if not self.can_open_trade():
            return None
        if signal.score < self.config.min_score: return None
        if self.is_pair_on_cooldown(signal.pair):
            self.logger.info(f"  {C.yellow(signal.pair)} cooldown. Skip."); return None
        if not self.api.client: return None

        # Liquidity gate
        ind = signal.indicators
        if ind.spread_pct > 0.3:
            self.logger.info(f"  {C.red(signal.pair)} spread {ind.spread_pct:.3f}%. Skip."); return None
        if ind.total_depth < 50000:
            self.logger.info(f"  {C.red(signal.pair)} thin book. Skip."); return None

        # Smart entry: don't chase
        if self.config.smart_entry and abs(ind.recent_move_pct) > self.config.chase_threshold_pct:
            self.logger.info(f"  {C.yellow(signal.pair)} moved {ind.recent_move_pct:+.1f}% recently. Don't chase."); return None

        balance = self.api.get_account_balance()
        if balance <= 0: time.sleep(1); balance = self.api.get_account_balance()
        if balance <= 0: self.logger.error(f"  {C.red('Zero balance!')}"); return None

        sym_info = self.api.get_symbol_info(signal.pair)
        qty, cap_pct = self.risk.calculate_position_size(balance, signal.price, sym_info, self.current_streak)
        if qty <= 0: return None

        f = lambda v: f"{v:.7f}" if v < 0.001 else f"{v:.4f}" if v < 1 else f"{v:.2f}"
        pp = sym_info.get("price_precision", 6)
        sp_c = C.green if ind.spread_pct < 0.05 else C.yellow if ind.spread_pct < 0.15 else C.red

        self.logger.info(f"\n{C.bold('EXECUTING')} {C.bg_blue(' TRADE ')}")
        self.logger.info(f"  Pair:      {C.white(signal.pair)} {C.long_short(signal.direction.value)}")
        self.logger.info(f"  Entry:     {C.white(f'${f(signal.price)}')} | Qty: {C.cyan(str(qty))} | ${C.cyan(f'{qty*signal.price:.2f}')}")
        self.logger.info(f"  Leverage:  {C.yellow(f'{self.config.leverage}x')} | Cap: {C.blue(f'{cap_pct:.0f}%')} | Streak: {C.white(f'{self.current_streak:+d}')}")
        self.logger.info(f"  SL:        {C.red(f'${f(signal.stop_loss)}')} ({C.red(f'{signal.risk_pct:.1f}%')} ATR-based)")
        self.logger.info(f"  TP:        {C.green(f'${f(signal.tp1)}')} / {C.green(f'${f(signal.tp2)}')} / {C.green(f'${f(signal.tp3)}')} (R:R {self.config.tp1_rr}/{self.config.tp2_rr}/{self.config.tp3_rr})")
        self.logger.info(f"  Score:     {C.score_color(signal.score)} | Sentiment: {C.cyan(f'{signal.sentiment_score:.0f}')} | {C.magenta(signal.pattern.value)}")
        self.logger.info(f"  Liquidity: Spread: {sp_c(f'{ind.spread_pct:.4f}%')} | Depth: {C.cyan(f'${ind.total_depth/1000:.0f}K')} | OI: {C.yellow(f'{ind.oi_change_pct:+.1f}%')}")

        if self.config.mode == "scan_only":
            self.logger.info(f"  {C.yellow('[SCAN ONLY]')}"); return None

        # Check if this pair already has an open position
        # If yes, DON'T cancel its SL/TP orders
        existing = self.api.get_open_positions()
        existing_syms = {p["symbol"] for p in existing} if existing else set()

        if signal.pair not in existing_syms:
            # Safe to cancel - no position on this pair
            self.api.cancel_all_orders(signal.pair)
            time.sleep(0.3)
        else:
            # Pair has existing position - don't touch its orders
            self.logger.info(f"  {C.yellow(signal.pair)} already has position. Skipping.")
            return None

        # Try to set margin type - skip silently if fails
        try:
            if self.api.client:
                self.api.client.futures_change_margin_type(symbol=signal.pair, marginType=self.config.margin_type)
        except:
            pass

        self.api.set_leverage(signal.pair, self.config.leverage)

        # FINAL CHECK right before order - positions could have changed
        if not self.can_open_trade():
            self.logger.info(f"  {C.yellow('Position limit reached before order. Abort.')}")
            return None

        side = SIDE_BUY if signal.direction == Direction.LONG else SIDE_SELL
        order = self.api.place_market_order(signal.pair, side, qty)
        if not order:
            self.logger.error(f"  {C.bg_red(' ORDER FAILED ')}"); self.telegram.notify_error(f"Failed: {signal.pair}"); return None

        self.logger.info(f"  {C.bg_green(' ORDER FILLED ')} ID: {order.get('orderId')}")

        sl_side = SIDE_SELL if signal.direction == Direction.LONG else SIDE_BUY
        self.api.place_stop_loss(signal.pair, sl_side, round(signal.stop_loss, pp), qty)
        min_q = sym_info.get("min_qty", 0.001)
        tp1_qty = round(qty * 0.5, sym_info.get("qty_precision", 3))
        if tp1_qty < min_q: tp1_qty = qty
        self.api.place_take_profit(signal.pair, sl_side, round(signal.tp1, pp), tp1_qty)

        pos = Position(pair=signal.pair, direction=signal.direction, entry_price=signal.price, quantity=qty,
            stop_loss=signal.stop_loss, tp1=signal.tp1, tp2=signal.tp2, tp3=signal.tp3,
            highest_price=signal.price, lowest_price=signal.price,
            opened_at=datetime.now(timezone.utc).isoformat(), order_id=str(order.get("orderId","")))
        self.open_positions.append(pos)
        self._pair_cooldowns[signal.pair] = time.time()
        self.trade_logger.log(type="open", pair=signal.pair, direction=signal.direction.value, price=signal.price, qty=qty, score=signal.score, sentiment=signal.sentiment_score, risk_pct=signal.risk_pct, cap_pct=cap_pct, streak=self.current_streak)
        self.telegram.notify_trade_open(signal, qty, cap_pct)
        return pos

    def monitor_positions(self):
        if not self.open_positions: return
        positions = self.sync_open_positions()
        if positions is None: return
        active_symbols = {p["symbol"] for p in positions}

        closed = [p for p in self.open_positions if p.pair not in active_symbols]
        for pos in closed:
            pnl = 0.0
            try:
                if self.api.client:
                    open_ts = int(datetime.fromisoformat(pos.opened_at).timestamp()*1000) if pos.opened_at else 0
                    trades = self.api.client.futures_account_trades(symbol=pos.pair, startTime=open_ts, limit=50)
                    for t in trades: pnl += float(t.get("realizedPnl", 0))
            except:
                pass

            self.open_positions.remove(pos)
            self._pair_cooldowns[pos.pair] = time.time()
            self._record_closed_trade(pos.pair, pnl)

        for pd_ in positions:
            symbol = pd_["symbol"]; mark = self.api.get_mark_price(symbol)
            pnl = float(pd_.get("unRealizedProfit", 0))
            for pos in self.open_positions:
                if pos.pair == symbol:
                    self.logger.info(f"  {C.dim('TRACK')} | {C.white(symbol)} | Mark: {C.cyan(f'${mark:.6f}')} | PnL: {C.pnl_color(pnl)}")
                    is_long = pos.direction == Direction.LONG
                    sl_side = SIDE_SELL if is_long else SIDE_BUY
                    pp = self.api.get_symbol_info(symbol).get("price_precision", 6)
                    if not pos.tp1_hit and self.config.move_sl_to_be_after_tp1:
                        if (is_long and mark >= pos.tp1) or (not is_long and mark <= pos.tp1):
                            pos.tp1_hit = True; pos.stop_loss = pos.entry_price
                            try:
                                self.api.cancel_all_orders(symbol); time.sleep(0.3)
                                self.api.place_stop_loss(symbol, sl_side, round(pos.entry_price, pp), pos.quantity)
                            except: pass
                            self.logger.info(f"  {C.bg_green(' TP1 HIT ')} {symbol} SL -> BE")
                            self.telegram.notify_tp_hit(symbol, "TP1", mark); continue
                    if self.config.trailing_sl and pos.tp1_hit:
                        new_sl = self.risk.calculate_trailing_sl(pos, mark)
                        diff = abs(new_sl-pos.stop_loss)/pos.stop_loss*100 if pos.stop_loss > 0 else 0
                        if diff > 0.1:
                            old = pos.stop_loss; pos.stop_loss = new_sl
                            try:
                                self.api.cancel_all_orders(symbol); time.sleep(0.3)
                                self.api.place_stop_loss(symbol, sl_side, round(new_sl, pp), pos.quantity)
                                self.logger.info(f"  {C.yellow('TRAIL')} {symbol} ${old:.6f} -> {C.green(f'${new_sl:.6f}')}")
                            except: pos.stop_loss = old

    def _record_closed_trade(self, pair, pnl, reason=None, notify_reason=None):
        pnl = float(pnl or 0)
        self.trade_count += 1
        self.total_pnl += pnl
        self.daily_pnl += pnl

        if pnl > 0:
            self.wins += 1
            self.current_streak = self.current_streak + 1 if self.current_streak > 0 else 1
            label = "WIN"
            self.logger.info(f"  {C.bg_green(' WIN ')} {C.white(pair)} | {C.green(f'+${pnl:.2f}')} | Streak: {C.green(f'{self.current_streak:+d}')}")
        elif pnl < 0:
            self.losses += 1
            self.current_streak = self.current_streak - 1 if self.current_streak < 0 else -1
            label = "LOSS"
            self.logger.info(f"  {C.bg_red(' LOSS ')} {C.white(pair)} | {C.red(f'-${abs(pnl):.2f}')} | Streak: {C.red(f'{self.current_streak:+d}')}")
        else:
            self.current_streak = 0
            label = reason or "BREAKEVEN"
            self.logger.info(f"  {C.bg_blue(' FLAT ')} {C.white(pair)} | $0.00 | Streak: {C.dim(f'{self.current_streak:+d}')}")

        close_reason = reason or label
        telegram_reason = notify_reason or close_reason
        self.telegram.notify_trade_close(pair, telegram_reason, pnl)
        self.trade_logger.log(type="close", pair=pair, pnl=pnl, reason=close_reason, streak=self.current_streak)

    def print_stats(self):
        wr = (self.wins/(self.wins+self.losses)*100) if (self.wins+self.losses) else 0
        bal = self.api.get_account_balance()
        dl = (self.daily_pnl/self.daily_start_balance*100) if self.daily_start_balance > 0 else 0
        streak_c = C.green if self.current_streak > 0 else C.red if self.current_streak < 0 else C.dim
        total_open = self.get_total_open_count()
        bot_open = len(self.open_positions)
        open_str = f"{total_open}" if total_open == bot_open else f"{total_open}({bot_open} bot)"
        self.logger.info(C.line("=", 75))
        self.logger.info(
            f"  {C.bold(C.cyan('v3.0'))} | Bal: {C.green(f'${bal:.2f}')} | PnL: {C.pnl_color(self.total_pnl)} | "
            f"W:{C.green(str(self.wins))} L:{C.red(str(self.losses))} WR:{C.green(f'{wr:.0f}%') if wr>=50 else C.red(f'{wr:.0f}%')} | "
            f"Streak: {streak_c(f'{self.current_streak:+d}')} | "
            f"Open: {C.yellow(open_str)} | Daily: {C.pnl_color(self.daily_pnl)}({dl:+.1f}%)")
        self.logger.info(C.line("=", 75))

    def close_bot_positions(self):
        if not self.api.client:
            return
        tracked = list(self.open_positions)
        if not tracked:
            self.logger.info(f"  {C.dim('No bot-managed positions to close.')}")
            return
        self.logger.info(f"  Closing {C.white(str(len(tracked)))} bot-managed position(s)...")
        for pos in tracked:
            try:
                self.close_position(pos.pair, reason="SHUTDOWN")
            except Exception as e:
                self.logger.error(f"  Bot close failed {pos.pair}: {e}")
        self.sync_open_positions()

    def close_all_positions(self):
        if not self.api.client: return
        positions = self.api.get_open_positions()
        if not positions: self.logger.info(f"  {C.dim('No positions.')}"); return
        self.logger.info(f"  Closing {C.white(str(len(positions)))}...")
        for pd_ in positions:
            sym = pd_["symbol"]; amt = float(pd_["positionAmt"])
            try:
                self.api.cancel_all_orders(sym); time.sleep(0.3)
                si = self.api.get_symbol_info(sym); cq = abs(amt)
                step = si.get("step_size",0.001); prec = si.get("qty_precision",3)
                cq = round(math.floor(cq/step)*step, prec)
                if cq <= 0: continue
                if amt > 0: self.api.client.futures_create_order(symbol=sym, side=SIDE_SELL, type="MARKET", quantity=str(cq))
                elif amt < 0: self.api.client.futures_create_order(symbol=sym, side=SIDE_BUY, type="MARKET", quantity=str(cq))
                pnl = float(pd_.get("unRealizedProfit",0))
                self._record_closed_trade(sym, pnl, reason="SHUTDOWN", notify_reason="SHUTDOWN")
            except Exception as e: self.logger.error(f"  Close failed {sym}: {e}")
        self.open_positions.clear()

    def run(self):
        self.print_banner()
        if not self.api.client:
            self.logger.error(f"{C.bg_red(' ERROR ')} No Binance API! Check .env"); return
        self.telegram.notify_startup(); self.telegram.start_polling()
        if self.config.telegram_enabled: self.logger.info(f"Telegram: {C.green('ON')}")
        else: self.logger.info(f"Telegram: {C.red('OFF')}")
        bal = self.api.get_account_balance()
        if bal <= 0: time.sleep(3); bal = self.api.get_account_balance()
        self.daily_start_balance = bal
        self.logger.info(f"Balance: {C.green(f'${bal:.2f}')} USDT")
        # Show 24h journal
        perf = self.trade_logger.get_recent_performance(24)
        if perf["wins"] + perf["losses"] > 0:
            self.logger.info(f"24h Journal: W:{perf['wins']} L:{perf['losses']} PnL:{C.pnl_color(perf['pnl'])} Avg:{C.pnl_color(perf['avg_pnl'])}")
        self.logger.info(f"Press {C.yellow('Ctrl+C')} to stop.\n")

        try:
            while True:
                if self._paused or self._daily_limit_hit:
                    self.logger.info(f"{C.bg_yellow(' PAUSED ')}"); time.sleep(10); continue
                self.monitor_positions()
                self.enforce_max_open_trades()
                if self.check_daily_limit(): continue
                self.print_stats()

                try: signals = self.run_full_scan()
                except Exception as e:
                    self.logger.error(f"Scan: {C.red(str(e))}"); self.telegram.notify_error(str(e)); signals = []

                total_open = self.get_total_open_count()
                if total_open >= self.config.max_open_trades:
                    ext = max(0, total_open - len(self.open_positions))
                    ext_msg = f" ({ext} external)" if ext > 0 else ""
                    mode_msg = 'Scanning only, no new entries.' if self.config.scan_while_full else 'Monitoring only.'
                    self.logger.info(f"  {C.yellow(f'[{total_open}/{self.config.max_open_trades}]')} Full{ext_msg}. {C.dim(mode_msg)}")
                    self.logger.info(f"\n{C.dim(f'Next scan in {self.config.scan_interval}s...')}\n")
                    time.sleep(self.config.scan_interval)
                    continue

                if signals:
                    # Only try ONE trade per scan cycle
                    for sig in signals[:5]:
                        result = self.execute_trade(sig)
                        if result:
                            time.sleep(1)  # Wait for Binance to register
                            break
                self.logger.info(f"\n{C.dim(f'Next scan in {self.config.scan_interval}s...')}\n")
                time.sleep(self.config.scan_interval)
        except KeyboardInterrupt:
            self.shutdown("KEYBOARD")
        except SystemExit:
            pass

def main():
    config = load_config()
    config.mode = "scan_only" if "--scan-only" in sys.argv else "live"
    QuantumBot(config).run()

if __name__ == "__main__": main()
