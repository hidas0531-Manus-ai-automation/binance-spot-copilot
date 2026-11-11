#!/usr/bin/env python3
"""
Binance Spot Trading Co-pilot  --  long-only, tiny-capital, rules-first.

STRATEGIES
  A  (1h)  Trend pullback. LIVE-capable.
           Close > EMA200 and > EMA50, RSI(14) crosses back above 40.
           Stop = entry - 1.5*ATR(14). Take profit = entry + 2 x risk.
  B  (15m) Bollinger mean-reversion. PAPER ONLY (never places real orders).

RULES ENFORCED IN CODE (search the log for "RULE-TRIGGER")
  1. Trade size is capped ($3 normally, $5 only if very few pairs qualify;
     $5 is a hard-coded ceiling no environment variable can exceed).
  2. Every trade has Entry / Stop / Take-profit decided BEFORE the order.
     The stop can never be moved further away (see move_stop()).
  3. Every profitable trade: 50% of NET profit goes to the Safety Pocket
     (an internal ledger). The bot never spends it.
     Emergency withdrawals from the ledger: CLI only, max 25% (lifetime).
  4. After 3 consecutive losses: live trading halts for 3 days (2 if the
     losses were minor). Only paper trading continues. A review file is
     generated, and live trading resumes only after YOU run `ack-review`.
  5. Anti-revenge-trading: cooldown after a loss, max trades per day,
     max one open live position, one signal per candle, no averaging down.
  6. Fees and slippage are included in every P&L figure (paper and live).

SAFETY
  * LIVE_TRADING defaults to false (paper only) until you set it to true.
  * The Exchange class below has NO withdraw/transfer methods.
  * At startup, the bot refuses to run live if the API key allows withdrawals.
  * Long-only Spot: it only ever BUYS then SELLS what it bought.

COMMANDS
  python bot.py                -> run the bot (same as `run`)
  python bot.py status         -> print ledger / streak / halt status
  python bot.py ack-review --notes "what I learned ..."
  python bot.py pocket-withdraw --amount 0.10 --reason "..." --i-understand
  python bot.py clear-pending  -> after manually checking Binance order history
"""

import argparse
import csv
import json
import logging
import math
import os
import signal
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from logging.handlers import RotatingFileHandler

try:
    import fcntl  # file locking (Linux/macOS, which includes Railway)
except ImportError:  # Windows: still works, just without the lock
    fcntl = None

try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
except ImportError:  # lets you read/test the code without the library
    Client = None

    class BinanceAPIException(Exception):
        pass


# =============================================================================
# CONFIGURATION  (all overridable with environment variables)
# =============================================================================
HARD_CEILING_USD = 5.00  # hard-coded: no setting can ever exceed this per trade


def _env(name, default):
    v = os.getenv(name)
    return default if v is None or v == "" else v


def env_float(name, default):
    return float(_env(name, default))


def env_int(name, default):
    return int(_env(name, default))


def env_bool(name, default):
    return str(_env(name, default)).strip().lower() in ("1", "true", "yes", "on")


class Config:
    # --- mode / infrastructure ---
    LIVE_TRADING = env_bool("LIVE_TRADING", False)  # False = paper only
    TESTNET = env_bool("BINANCE_TESTNET", False)
    DATA_DIR = _env("DATA_DIR", "./data")  # on Railway: mount a volume here
    API_KEY = os.getenv("BINANCE_API_KEY")
    API_SECRET = os.getenv("BINANCE_API_SECRET")

    # --- universe ---
    SYMBOLS = [s.strip().upper() for s in _env(
        "SYMBOLS",
        "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,TRXUSDT,LINKUSDT,AVAXUSDT",
    ).split(",") if s.strip()]
    QUOTE = "USDT"

    # --- sizing ---
    MAX_TRADE_USD = min(env_float("MAX_TRADE_USD", 3.0), HARD_CEILING_USD)
    FALLBACK_TRADE_USD = min(env_float("FALLBACK_MAX_TRADE_USD", 5.0), HARD_CEILING_USD)
    MIN_QUALIFYING_PAIRS = env_int("MIN_QUALIFYING_PAIRS", 2)  # fewer => use fallback cap
    MIN_TRADE_USD = 1.0
    STARTING_CAPITAL = env_float("STARTING_CAPITAL", 10.0)  # trading pool, first run only
    # A stop-loss exit must still be >= Binance's min notional, so a pair only
    # qualifies if min_notional * EXIT_HEADROOM <= the cap.
    EXIT_HEADROOM = env_float("EXIT_HEADROOM", 1.08)

    # --- Strategy A (1h trend pullback) ---
    TF_A = "1h"
    EMA_TREND, EMA_PULL = 200, 50
    RSI_LEN, RSI_LEVEL = 14, 40.0
    ATR_LEN, ATR_STOP_MULT, REWARD_RISK = 14, 1.5, 2.0

    # --- Strategy B (15m Bollinger, paper only) ---
    TF_B = "15m"
    BB_LEN, BB_MULT, RSI_B = 20, 2.0, 30.0
    FLAT_LOOKBACK, FLAT_PCT = 48, 0.005  # EMA200 moved < 0.5% over 12h = "flat"
    ATR_B_MULT = 1.0

    # --- costs (used in paper P&L and plan validation) ---
    FEE_RATE = env_float("FEE_RATE", 0.001)  # 0.10% per side (0.075% with BNB)
    SLIPPAGE = env_float("SLIPPAGE", 0.0005)  # 0.05% assumed per market order
    MIN_TP_PCT = env_float("MIN_TP_PCT", 0.006)  # skip trades whose target < 0.6%
    MAX_STOP_PCT = env_float("MAX_STOP_PCT", 0.05)  # skip trades whose stop > 5%
    MAX_ENTRY_DRIFT_PCT = 0.005  # don't chase if price ran >0.5% above signal

    # --- profit allocation ---
    PROFIT_LOCK_PCT = 0.50
    EMERGENCY_MAX_PCT = 0.25

    # --- consecutive-loss protocol ---
    LOSS_STREAK_LIMIT = 3
    HALT_DAYS_DEFAULT = 3
    HALT_DAYS_MINOR = 2
    MINOR_LOSS_PCT = env_float("MINOR_LOSS_PCT", 0.02)  # avg loss <= 2% of cost = "minor"

    # --- anti revenge trading ---
    COOLDOWN_AFTER_LOSS_MIN = env_int("COOLDOWN_AFTER_LOSS_MIN", 120)
    MAX_TRADES_PER_DAY = env_int("MAX_TRADES_PER_DAY", 4)

    # --- loop timing ---
    POLL_SECONDS = env_int("POLL_SECONDS", 30)
    SIGNAL_MAX_AGE_SEC = 300  # ignore signals from a candle that closed >5 min ago
    UNIVERSE_REFRESH_SEC = 6 * 3600
    HEARTBEAT_MIN = env_int("HEARTBEAT_MIN", 60)


TF_MS = {"15m": 15 * 60 * 1000, "1h": 60 * 60 * 1000}


def P(filename):
    """Path inside the data directory."""
    return os.path.join(Config.DATA_DIR, filename)


log = logging.getLogger("bot")


def setup_logging():
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    if log.handlers:
        return
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    fh = RotatingFileHandler(P("bot.log"), maxBytes=2_000_000, backupCount=3)
    fh.setFormatter(fmt)
    log.addHandler(fh)


def utcnow():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.isoformat(timespec="seconds")


def parse_iso(s):
    return datetime.fromisoformat(s)


# =============================================================================
# INDICATORS (plain Python, no pandas needed)
# =============================================================================
def ema(values, length):
    k = 2.0 / (length + 1)
    out, prev = [], None
    for i, v in enumerate(values):
        if i < length - 1:
            out.append(None)
            continue
        prev = sum(values[:length]) / length if i == length - 1 else v * k + prev * (1 - k)
        out.append(prev)
    return out


def _rsi_value(avg_gain, avg_loss):
    return 100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def rsi(closes, length=14):
    """Wilder's RSI."""
    out = [None] * len(closes)
    if len(closes) <= length:
        return out
    gains = [max(closes[i] - closes[i - 1], 0.0) for i in range(1, length + 1)]
    losses = [max(closes[i - 1] - closes[i], 0.0) for i in range(1, length + 1)]
    ag, al = sum(gains) / length, sum(losses) / length
    out[length] = _rsi_value(ag, al)
    for i in range(length + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (length - 1) + max(d, 0.0)) / length
        al = (al * (length - 1) + max(-d, 0.0)) / length
        out[i] = _rsi_value(ag, al)
    return out


def atr(highs, lows, closes, length=14):
    """Wilder's Average True Range."""
    n = len(closes)
    out = [None] * n
    if n <= length:
        return out
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
           for i in range(1, n)]  # trs[j] belongs to candle j+1
    a = sum(trs[:length]) / length
    out[length] = a
    for i in range(length + 1, n):
        a = (a * (length - 1) + trs[i - 1]) / length
        out[i] = a
    return out


def bollinger(closes, length=20, mult=2.0):
    n = len(closes)
    mid, lower = [None] * n, [None] * n
    for i in range(length - 1, n):
        w = closes[i - length + 1: i + 1]
        m = sum(w) / length
        sd = math.sqrt(sum((x - m) ** 2 for x in w) / length)
        mid[i], lower[i] = m, m - mult * sd
    return mid, lower


# =============================================================================
# STRATEGY SIGNALS  (pure functions: candles in, trade plan out)
# candles = {"t": [open_ms], "o": [], "h": [], "l": [], "c": []}  closed only
# A plan has: ref (reference entry), stop_dist, tp_dist (both in price units)
# =============================================================================
def signal_a(cd):
    c, h, l = cd["c"], cd["h"], cd["l"]
    if len(c) < Config.EMA_TREND + 10:
        return None
    e200, e50 = ema(c, Config.EMA_TREND), ema(c, Config.EMA_PULL)
    r, a = rsi(c, Config.RSI_LEN), atr(h, l, c, Config.ATR_LEN)
    if None in (e200[-1], e50[-1], r[-1], r[-2], a[-1]):
        return None
    px = c[-1]
    if not (px > e200[-1] and px > e50[-1]):
        return None
    if not (r[-2] < Config.RSI_LEVEL <= r[-1]):
        return None
    stop_dist = Config.ATR_STOP_MULT * a[-1]
    return {"strategy": "A", "ref": px, "stop_dist": stop_dist,
            "tp_dist": Config.REWARD_RISK * stop_dist,
            "ctx": {"rsi_prev": round(r[-2], 1), "rsi": round(r[-1], 1), "atr": a[-1],
                    "ema50": e50[-1], "ema200": e200[-1],
                    "pct_above_ema200": round((px / e200[-1] - 1) * 100, 2)}}


def signal_b(cd):
    c, h, l = cd["c"], cd["h"], cd["l"]
    need = Config.EMA_TREND + Config.FLAT_LOOKBACK + 5
    if len(c) < need:
        return None
    e200 = ema(c, Config.EMA_TREND)
    mid, lower = bollinger(c, Config.BB_LEN, Config.BB_MULT)
    r, a = rsi(c, Config.RSI_LEN), atr(h, l, c, Config.ATR_LEN)
    if None in (e200[-1], e200[-1 - Config.FLAT_LOOKBACK], mid[-1], lower[-1], r[-1], a[-1]):
        return None
    slope = abs(e200[-1] - e200[-1 - Config.FLAT_LOOKBACK]) / e200[-1]
    if slope >= Config.FLAT_PCT:
        return None
    px = c[-1]
    if not (px < lower[-1] and r[-1] < Config.RSI_B):
        return None
    stop_dist = px - (l[-1] - Config.ATR_B_MULT * a[-1])
    tp_dist = mid[-1] - px
    if stop_dist <= 0 or tp_dist <= 0:
        return None
    return {"strategy": "B", "ref": px, "stop_dist": stop_dist, "tp_dist": tp_dist,
            "ctx": {"rsi": round(r[-1], 1), "atr": a[-1], "ema200_slope_pct": round(slope * 100, 3)}}


# =============================================================================
# HELPERS: rounding, exchange-filter parsing
# =============================================================================
def floor_to_step(qty, step):
    return (qty // step) * step


def parse_symbol(s):
    f = {x["filterType"]: x for x in s.get("filters", [])}
    mn = f.get("NOTIONAL") or f.get("MIN_NOTIONAL")
    lot = f.get("LOT_SIZE", {"stepSize": "0.00000001", "minQty": "0"})
    return {
        "symbol": s["symbol"], "base": s["baseAsset"], "quote": s["quoteAsset"],
        "min_notional": float(mn["minNotional"]) if mn else 0.0,
        "step": Decimal(lot["stepSize"]), "min_qty": Decimal(lot["minQty"]),
        "ok": s.get("status") == "TRADING" and s.get("isSpotTradingAllowed", True)
              and "MARKET" in s.get("orderTypes", ["MARKET"]),
    }


# =============================================================================
# EXCHANGE WRAPPER -- the ONLY place that talks to Binance.
# There is deliberately no withdraw, transfer, margin or futures method here.
# =============================================================================
class Exchange:
    def __init__(self, key, secret, testnet=False):
        if Client is None:
            raise RuntimeError("python-binance is not installed (pip install -r requirements.txt)")
        self.c = Client(key, secret, testnet=testnet)

    def exchange_info(self):
        return self.c.get_exchange_info()

    def klines(self, symbol, interval, limit=300):
        return self.c.get_klines(symbol=symbol, interval=interval, limit=limit)

    def price(self, symbol):
        return float(self.c.get_symbol_ticker(symbol=symbol)["price"])

    def free_balance(self, asset):
        b = self.c.get_asset_balance(asset=asset)
        return float(b["free"]) if b else 0.0

    def api_permissions(self):
        return self.c.get_account_api_permissions()

    def market_buy_quote(self, symbol, quote_usd):
        if quote_usd > HARD_CEILING_USD:
            raise RuntimeError(f"REFUSED: order ${quote_usd} exceeds hard ceiling ${HARD_CEILING_USD}")
        amt = Decimal(str(quote_usd)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        return self.c.create_order(symbol=symbol, side="BUY", type="MARKET",
                                   quoteOrderQty=format(amt, "f"), newOrderRespType="FULL")

    def market_sell_qty(self, symbol, qty):
        return self.c.create_order(symbol=symbol, side="SELL", type="MARKET",
                                   quantity=format(qty, "f"), newOrderRespType="FULL")


# =============================================================================
# PERSISTENT STATE
# =============================================================================
def new_state():
    zero = lambda: {"n": 0, "wins": 0, "pnl": 0.0}
    return {
        "version": 1,
        "pool_usd": Config.STARTING_CAPITAL,
        "pocket": {"balance": 0.0, "lifetime_deposited": 0.0, "lifetime_withdrawn": 0.0},
        "consecutive_losses": 0, "recent_losses": [], "last_loss_time": None,
        "halt_until": None, "halt_days": None,
        "review_required": False, "review_acknowledged": False, "review_file": None,
        "pending_order": None,
        "live_position": None, "paper_positions": {},
        "live_stats": zero(), "paper_stats": {"A": zero(), "B": zero()},
        "trades_today": {"date": "", "count": 0},
        "last_eval": {},
    }


class StateStore:
    def __init__(self, path):
        self.path = path
        self.lock_path = path + ".lock"

    def _load(self):
        base = new_state()
        if not os.path.exists(self.path):
            return base
        with open(self.path) as f:
            st = json.load(f)
        for k, v in base.items():
            st.setdefault(k, v)
        return st

    def flush(self, st):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f, indent=2, default=str)
        os.replace(tmp, self.path)

    @contextmanager
    def transaction(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        lock = open(self.lock_path, "w")
        try:
            if fcntl:
                fcntl.flock(lock, fcntl.LOCK_EX)
            st = self._load()
            try:
                yield st
            finally:
                self.flush(st)
        finally:
            if fcntl:
                fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()


TRADE_COLUMNS = ["closed_at", "mode", "strategy", "symbol", "entry", "exit", "qty", "cost_usd",
                 "net_pnl_usd", "pnl_pct", "fees_usd", "exit_reason", "stop", "tp", "opened_at", "context"]


def append_trade_csv(trade):
    path = P("trades.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRADE_COLUMNS, extrasaction="ignore")
        if new:
            w.writeheader()
        row = dict(trade)
        row["context"] = json.dumps(trade.get("ctx", {}))
        w.writerow(row)


def move_stop(pos, new_stop):
    """A stop may only move closer to price (up for a long). Never looser."""
    if new_stop < pos["stop"]:
        log.error("RULE-TRIGGER | STOP WIDENING REFUSED | %s old=%.8g new=%.8g",
                  pos["symbol"], pos["stop"], new_stop)
        return False
    pos["stop"] = new_stop
    return True


class Bot:
    def __init__(self):
        self.store = StateStore(P("state.json"))
        self.ex = None
        self.infos = {}
        self.universe = {}
        self.cap = Config.MAX_TRADE_USD
        self.universe_ts = 0.0
        self.running = True
        self.last_heartbeat = 0.0

    def startup(self):
        setup_logging()
        log.info("=" * 70)
        log.info("STARTING | live_trading=%s testnet=%s data_dir=%s", Config.LIVE_TRADING,
                 Config.TESTNET, os.path.abspath(Config.DATA_DIR))
        if Config.LIVE_TRADING and not (Config.API_KEY and Config.API_SECRET):
            sys.exit("LIVE_TRADING=true but BINANCE_API_KEY / BINANCE_API_SECRET are not set.")
        self.ex = Exchange(Config.API_KEY, Config.API_SECRET, Config.TESTNET)
        if Config.LIVE_TRADING and not Config.TESTNET:
            self.check_api_permissions()
        self.refresh_universe()
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)
        with self.store.transaction() as st:
            if st["pending_order"]:
                log.critical("UNRESOLVED ORDER MARKER for %s. Live trading is blocked. Check your Binance "
                             "order history, then run: python bot.py clear-pending",
                             st["pending_order"].get("symbol"))
            if st["halt_until"]:
                log.warning("RULE-TRIGGER | LEARNING PERIOD ACTIVE until %s (paper only)", st["halt_until"])

    def _stop(self, *_):
        log.info("Shutdown signal received; finishing current loop...")
        self.running = False

    def check_api_permissions(self):
        try:
            perms = self.ex.api_permissions()
        except Exception as e:
            log.warning("Could not verify API permissions (%s). Please confirm in Binance that withdrawals "
                        "are DISABLED for this key.", e)
            return
        if perms.get("enableWithdrawals"):
            sys.exit("SAFETY STOP: this API key has WITHDRAWALS ENABLED. Disable that in Binance API "
                     "Management (or create a new key) and restart.")
        log.info("API key check OK: withdrawals disabled, spot trading=%s",
                 perms.get("enableSpotAndMarginTrading"))

    def refresh_universe(self):
        raw = {s["symbol"]: parse_symbol(s) for s in self.ex.exchange_info()["symbols"]}
        self.infos = {sym: raw[sym] for sym in Config.SYMBOLS if sym in raw}
        for sym in Config.SYMBOLS:
            if sym not in raw:
                log.warning("SKIP %s | symbol not found on Binance Spot", sym)

        def qualifies(info, cap):
            return info["ok"] and info["quote"] == Config.QUOTE and \
                   info["min_notional"] * Config.EXIT_HEADROOM <= cap

        def report(cap):
            for sym, i in self.infos.items():
                if not qualifies(i, cap):
                    log.warning("SKIP %s | min notional $%.2f (needs $%.2f incl. stop-exit headroom) "
                                "> cap $%.2f", sym, i["min_notional"], i["min_notional"] * Config.EXIT_HEADROOM, cap)

        cap = Config.MAX_TRADE_USD
        ok = {s: i for s, i in self.infos.items() if qualifies(i, cap)}
        report(cap)
        if len(ok) < Config.MIN_QUALIFYING_PAIRS and Config.FALLBACK_TRADE_USD > cap:
            log.warning("RULE-TRIGGER | FALLBACK CAP | only %d pair(s) qualify at $%.2f; temporarily raising "
                        "max trade size to $%.2f", len(ok), cap, Config.FALLBACK_TRADE_USD)
            cap = Config.FALLBACK_TRADE_USD
            ok = {s: i for s, i in self.infos.items() if qualifies(i, cap)}
            report(cap)
        self.cap, self.universe = cap, ok
        self.universe_ts = time.time()
        if ok:
            log.info("UNIVERSE | cap=$%.2f | tradable: %s", cap, ", ".join(
                f"{s}(min ${i['min_notional']:.2f})" for s, i in ok.items()))
        else:
            log.error("UNIVERSE EMPTY | no pair fits under $%.2f. The bot will idle. Add cheaper-minimum "
                      "pairs to SYMBOLS or review the cap.", cap)

    def run(self):
        self.startup()
        backoff = 5
        while self.running:
            try:
                with self.store.transaction() as st:
                    self.tick(st)
                backoff = 5
                self._sleep(Config.POLL_SECONDS)
            except Exception:
                log.exception("Loop error; retrying in %ss", backoff)
                self._sleep(backoff)
                backoff = min(backoff * 2, 300)
        log.info("Stopped cleanly.")

    def _sleep(self, seconds):
        end = time.time() + seconds
        while self.running and time.time() < end:
            time.sleep(1)

    def tick(self, st):
        now = utcnow()
        today = now.strftime("%Y-%m-%d")
        if st["trades_today"]["date"] != today:
            st["trades_today"] = {"date": today, "count": 0}
        if time.time() - self.universe_ts > Config.UNIVERSE_REFRESH_SEC:
            self.refresh_universe()
        self.update_learning_period(st, now)
        self.manage_positions(st)
        self.scan_signals(st, now)
        self.heartbeat(st, now)

    def update_learning_period(self, st, now):
        h = st["halt_until"]
        if not h or now < parse_iso(h):
            return
        if st["review_acknowledged"]:
            log.warning("RULE-TRIGGER | LEARNING PERIOD COMPLETE | lessons reviewed. Live trading resumes.")
            st.update(halt_until=None, halt_days=None, review_required=False, review_acknowledged=False,
                      consecutive_losses=0, recent_losses=[])
        elif time.time() - self.last_heartbeat > 600:
            log.warning("RULE-TRIGGER | HALT EXPIRED but review not acknowledged. Read %s then run: "
                        "python bot.py ack-review --notes \"...\"", st["review_file"])

    def trigger_halt(self, st, now):
        losses = st["recent_losses"]
        total_loss = sum(-t["net_pnl"] for t in losses)
        total_cost = sum(t["cost_usd"] for t in losses) or 1.0
        avg_loss_pct = total_loss / total_cost
        days = Config.HALT_DAYS_MINOR if avg_loss_pct <= Config.MINOR_LOSS_PCT else Config.HALT_DAYS_DEFAULT
        st["halt_until"] = iso(now + timedelta(days=days))
        st["halt_days"] = days
        st["review_required"], st["review_acknowledged"] = True, False
        st["review_file"] = self.write_review(st, losses, days, now)
        log.error("RULE-TRIGGER | %d CONSECUTIVE LOSSES | live trading HALTED for %d days until %s "
                  "(avg loss %.2f%% of cost). Paper trading continues. Review file: %s",
                  Config.LOSS_STREAK_LIMIT, days, st["halt_until"], avg_loss_pct * 100, st["review_file"])

    def write_review(self, st, losses, days, now):
        os.makedirs(P("reviews"), exist_ok=True)
        path = P(f"reviews/review_{now.strftime('%Y%m%d_%H%M')}.md")
        lines = [f"# Post-trade review ({now.strftime('%Y-%m-%d %H:%M UTC')})",
                 f"Live trading halted {days} days (until {st['halt_until']}).",
                 "Fill in the notes, then run: `python bot.py ack-review --notes \"...\"`", ""]
        for i, t in enumerate(losses, 1):
            ctx = t.get("ctx", {})
            lines += [f"## Losing trade {i}: {t['symbol']}",
                      f"- Opened {t['opened_at']}  Closed {t['closed_at']}  Exit: {t['exit_reason']}",
                      f"- Entry {t['entry']:.8g}  Exit {t['exit']:.8g}  Stop {t['stop']:.8g}  TP {t['tp']:.8g}",
                      f"- Net P&L ${t['net_pnl']:.4f} ({t['pnl_pct']*100:.2f}%)  Fees ${t['fees_usd']:.4f}",
                      f"- Signal context: {json.dumps(ctx)}", ""]
        lines += ["## Structured analysis (write your answers)",
                  "1. Entry quality: was RSI/pullback clean? Was price extended above EMA50?",
                  "2. Stop placement: too tight (noise) or too loose? Hit soon after entry?",
                  "3. Market context: broad market falling? News? Low volume / chop?",
                  "4. False signals: what did the three signals have in common?",
                  "5. Costs: how much did fees + slippage eat of each target?",
                  "6. Discipline: any rule broken or any urge to revenge-trade?",
                  "7. Key lessons / one change to test in paper mode:", ""]
        with open(path, "w") as f:
            f.write("\n".join(lines))
        return path

    def mode_for_a(self, st, now):
        if not Config.LIVE_TRADING:
            return "paper", "LIVE_TRADING is off"
        if st["halt_until"]:
            return "paper", "learning period: paper/practice only"
        if st["pending_order"]:
            return "paper", "unresolved order marker (run clear-pending after checking Binance)"
        if st["live_position"]:
            return "skip", "a live position is already open (max 1)"
        if st["trades_today"]["count"] >= Config.MAX_TRADES_PER_DAY:
            return "skip", f"daily trade limit ({Config.MAX_TRADES_PER_DAY}) reached"
        ll = st["last_loss_time"]
        if ll and (now - parse_iso(ll)).total_seconds() < Config.COOLDOWN_AFTER_LOSS_MIN * 60:
            return "skip", "post-loss cooldown (anti revenge-trading)"
        if st["pool_usd"] < Config.MIN_TRADE_USD:
            return "skip", "trading pool below $1"
        return "live", ""

    def get_candles(self, symbol, interval):
        raw = self.ex.klines(symbol, interval, 300)
        now_ms = int(time.time() * 1000)
        rows = [r for r in raw if r[6] < now_ms]
        return {"t": [r[0] for r in rows], "o": [float(r[1]) for r in rows],
                "h": [float(r[2]) for r in rows], "l": [float(r[3]) for r in rows],
                "c": [float(r[4]) for r in rows]}

    def scan_signals(self, st, now):
        now_ms = int(now.timestamp() * 1000)
        for sym, info in self.universe.items():
            for strat, tf in (("A", Config.TF_A), ("B", Config.TF_B)):
                tf_ms = TF_MS[tf]
                boundary = (now_ms // tf_ms) * tf_ms
                age_ms = now_ms - boundary
                if age_ms < 3000:
                    continue
                latest_open = boundary - tf_ms
                key = f"{strat}:{sym}"
                if st["last_eval"].get(key) == latest_open:
                    continue
                if age_ms > Config.SIGNAL_MAX_AGE_SEC * 1000:
                    st["last_eval"][key] = latest_open
                    continue
                cd = self.get_candles(sym, tf)
                if not cd["t"] or cd["t"][-1] != latest_open:
                    continue
                st["last_eval"][key] = latest_open
                sig = signal_a(cd) if strat == "A" else signal_b(cd)
                if not sig:
                    continue
                sig["symbol"] = sym
                log.info("SIGNAL | %s %s | ref=%.8g stop_dist=%.8g (%.2f%%) tp_dist=%.8g (%.2f%%) %s",
                         strat, sym, sig["ref"], sig["stop_dist"], sig["stop_dist"] / sig["ref"] * 100,
                         sig["tp_dist"], sig["tp_dist"] / sig["ref"] * 100, json.dumps(sig["ctx"]))
                if strat == "B":
                    self.open_paper(st, "B", info, sig)
                    continue
                mode, why = self.mode_for_a(st, now)
                if mode == "live":
                    self.open_live(st, info, sig, now)
                elif mode == "paper":
                    self.open_paper(st, "A", info, sig)
                else:
                    log.info("SIGNAL SKIPPED | A %s | %s", sym, why)

    def validate_plan(self, sig, info, size, price_now):
        entry = sig["ref"]
        stop_pct, tp_pct = sig["stop_dist"] / entry, sig["tp_dist"] / entry
        if size < Config.MIN_TRADE_USD:
            return False, f"size ${size:.2f} below $1 minimum"
        if stop_pct > Config.MAX_STOP_PCT:
            return False, f"stop {stop_pct:.2%} wider than allowed {Config.MAX_STOP_PCT:.0%}"
        if tp_pct < Config.MIN_TP_PCT:
            return False, f"target {tp_pct:.2%} too small after fees (min {Config.MIN_TP_PCT:.1%})"
        if size + 1e-9 < info["min_notional"]:
            return False, f"size ${size:.2f} below min notional ${info['min_notional']:.2f}"
        exit_notional = size * (1 - stop_pct - Config.SLIPPAGE) * (1 - Config.FEE_RATE)
        if exit_notional < info["min_notional"]:
            return False, (f"a stop-loss exit (~${exit_notional:.2f}) would fall below min notional "
                           f"${info['min_notional']:.2f}; position could get stuck")
        if size / price_now * (1 - stop_pct) < float(info["min_qty"]):
            return False, "quantity would fall below LOT_SIZE minimum"
        if price_now > entry * (1 + Config.MAX_ENTRY_DRIFT_PCT):
            return False, "price already ran >0.5% above the signal; not chasing"
        return True, ""

    def explicit_fee_usd(self, fills, embedded_asset, ref_price):
        total = 0.0
        for f in fills or []:
            asset, amt = f["commissionAsset"], float(f["commission"])
            if asset == embedded_asset:
                continue
            if asset == Config.QUOTE:
                total += amt
            elif asset == "BNB":
                try:
                    total += amt * self.ex.price("BNBUSDT")
                except Exception:
                    log.warning("Could not price BNB fee; P&L slightly optimistic for this trade")
            else:
                total += amt * ref_price
        return total

    def open_live(self, st, info, sig, now):
        sym = sig["symbol"]
        price_now = self.ex.price(sym)
        free = self.ex.free_balance(Config.QUOTE)
        spendable = min(st["pool_usd"], free - st["pocket"]["balance"])
        size = math.floor(min(self.cap, spendable) * 100) / 100
        ok, why = self.validate_plan(sig, info, size, price_now)
        if not ok:
            log.warning("TRADE REJECTED (plan check) | A %s | %s", sym, why)
            return
        stop_planned, tp_planned = price_now - sig["stop_dist"], price_now + sig["tp_dist"]
        log.info("TRADE PLAN | LIVE A %s | entry~%.8g stop=%.8g tp=%.8g size=$%.2f (pool $%.2f, pocket "
                 "$%.2f excluded)", sym, price_now, stop_planned, tp_planned, size, st["pool_usd"],
                 st["pocket"]["balance"])
        st["pending_order"] = {"symbol": sym, "side": "BUY", "ts": iso(now)}
        self.store.flush(st)
        try:
            resp = self.ex.market_buy_quote(sym, size)
        except BinanceAPIException as e:
            st["pending_order"] = None
            log.error("BUY REJECTED by Binance | %s | %s", sym, e)
            return
        except Exception as e:
            log.critical("BUY OUTCOME UNKNOWN | %s | %s. Live trading blocked until you check Binance and "
                         "run: python bot.py clear-pending", sym, e)
            return
        executed = Decimal(resp["executedQty"])
        if executed <= 0:
            st["pending_order"] = None
            log.error("BUY returned zero fill | %s | %s", sym, resp.get("status"))
            return
        spent = float(resp["cummulativeQuoteQty"])
        avg = spent / float(executed)
        base_fee = sum(Decimal(f["commission"]) for f in resp.get("fills", [])
                       if f["commissionAsset"] == info["base"])
        net_qty = executed - base_fee
        fee_in = self.explicit_fee_usd(resp.get("fills"), info["base"], avg)
        pos = {"symbol": sym, "strategy": "A", "mode": "live", "opened_at": iso(now),
               "entry": avg, "qty": format(net_qty, "f"), "cost": spent + fee_in, "fee_in": fee_in,
               "stop": avg - sig["stop_dist"], "tp": avg + sig["tp_dist"],
               "stop_dist": sig["stop_dist"], "tp_dist": sig["tp_dist"],
               "ref": sig["ref"], "ctx": sig["ctx"], "sell_failures": 0}
        st["live_position"] = pos
        st["pending_order"] = None
        st["pool_usd"] -= pos["cost"]
        st["trades_today"]["count"] += 1
        log.info("TRADE OPENED | LIVE A %s | fill=%.8g (slippage vs signal %.3f%%) qty=%s stop=%.8g "
                 "tp=%.8g cost=$%.4f", sym, avg, (avg / sig["ref"] - 1) * 100, pos["qty"], pos["stop"],
                 pos["tp"], pos["cost"])

    def close_live(self, st, pos, reason, price):
        sym = pos["symbol"]
        info = self.infos[sym]
        log.info("EXIT TRIGGER | LIVE %s | %s at ~%.8g", sym, reason, price)
        try:
            bal = Decimal(str(self.ex.free_balance(info["base"])))
            qty = floor_to_step(min(Decimal(pos["qty"]), bal), info["step"])
            if qty < info["min_qty"] or float(qty) * price < info["min_notional"]:
                raise ValueError(f"sell size {qty} (~${float(qty) * price:.2f}) is below Binance minimums")
            resp = self.ex.market_sell_qty(sym, qty)
        except Exception as e:
            pos["sell_failures"] += 1
            if pos["sell_failures"] % 20 == 1:
                log.critical("SELL FAILED | %s | %s | attempt %d. Position still open; retrying. If this "
                             "persists, close it manually on Binance.", sym, e, pos["sell_failures"])
            return
        got = float(resp["cummulativeQuoteQty"])
        sold = Decimal(resp["executedQty"])
        avg = got / float(sold) if sold else price
        fee_out = self.explicit_fee_usd(resp.get("fills"), None, avg)
        st["live_position"] = None
        self.finalize_trade(st, pos, avg, got - fee_out, fee_out, reason)

    def open_paper(self, st, strat, info, sig):
        sym = sig["symbol"]
        key = f"{strat}:{sym}"
        if key in st["paper_positions"]:
            return
        price_now = self.ex.price(sym)
        size = self.cap
        ok, why = self.validate_plan(sig, info, size, price_now)
        if not ok:
            log.info("PAPER REJECTED | %s %s | %s", strat, sym, why)
            return
        fill = price_now * (1 + Config.SLIPPAGE)
        fee_in = size * Config.FEE_RATE
        st["paper_positions"][key] = {
            "symbol": sym, "strategy": strat, "mode": "paper", "opened_at": iso(utcnow()),
            "entry": fill, "qty": size / fill, "cost": size + fee_in, "fee_in": fee_in,
            "stop": fill - sig["stop_dist"], "tp": fill + sig["tp_dist"],
            "stop_dist": sig["stop_dist"], "tp_dist": sig["tp_dist"], "ref": sig["ref"], "ctx": sig["ctx"]}
        p = st["paper_positions"][key]
        log.info("TRADE OPENED | PAPER %s %s | fill=%.8g stop=%.8g tp=%.8g size=$%.2f",
                 strat, sym, fill, p["stop"], p["tp"], size)

    def close_paper(self, st, key, pos, reason, price):
        exit_px = price * (1 - Config.SLIPPAGE)
        gross = pos["qty"] * exit_px
        fee_out = gross * Config.FEE_RATE
        del st["paper_positions"][key]
        self.finalize_trade(st, pos, exit_px, gross - fee_out, fee_out, reason)

    @staticmethod
    def exit_reason(pos, price):
        if price <= pos["stop"]:
            return "stop_loss"
        if price >= pos["tp"]:
            return "take_profit"
        return None

    def manage_positions(self, st):
        pos = st["live_position"]
        if pos:
            price = self.ex.price(pos["symbol"])
            reason = self.exit_reason(pos, price)
            if reason:
                self.close_live(st, pos, reason, price)
        for key, pos in list(st["paper_positions"].items()):
            price = self.ex.price(pos["symbol"])
            reason = self.exit_reason(pos, price)
            if reason:
                self.close_paper(st, key, pos, reason, price)

    def finalize_trade(self, st, pos, exit_avg, proceeds_net, fee_out, reason):
        now = utcnow()
        pnl = proceeds_net - pos["cost"]
        trade = {"closed_at": iso(now), "opened_at": pos["opened_at"], "mode": pos["mode"],
                 "strategy": pos["strategy"], "symbol": pos["symbol"], "entry": pos["entry"],
                 "exit": exit_avg, "qty": float(pos["qty"]), "cost_usd": pos["cost"],
                 "proceeds_net": proceeds_net, "net_pnl": pnl, "net_pnl_usd": round(pnl, 6),
                 "pnl_pct": pnl / pos["cost"], "fees_usd": pos.get("fee_in", 0) + fee_out,
                 "exit_reason": reason, "stop": pos["stop"], "tp": pos["tp"], "ctx": pos.get("ctx", {})}
        append_trade_csv(trade)
        log.info("TRADE CLOSED | %s %s %s | %s | entry=%.8g exit=%.8g | net P&L $%.4f (%.2f%%) | fees $%.4f",
                 pos["mode"].upper(), pos["strategy"], pos["symbol"], reason, pos["entry"], exit_avg,
                 pnl, trade["pnl_pct"] * 100, trade["fees_usd"])
        if pos["mode"] == "paper":
            s = st["paper_stats"][pos["strategy"]]
            s["n"] += 1
            s["pnl"] += pnl
            s["wins"] += 1 if pnl > 0 else 0
            return
        self.apply_live_result(st, trade, now)

    def apply_live_result(self, st, trade, now):
        pnl, proceeds = trade["net_pnl"], trade["proceeds_net"]
        s = st["live_stats"]
        s["n"] += 1
        s["pnl"] += pnl
        if pnl > 0:
            s["wins"] += 1
            lock = round(pnl * Config.PROFIT_LOCK_PCT, 8)
            pk = st["pocket"]
            pk["balance"] += lock
            pk["lifetime_deposited"] += lock
            st["pool_usd"] += proceeds - lock
            st["consecutive_losses"], st["recent_losses"] = 0, []
            log.info("RULE-TRIGGER | PROFIT LOCK | net profit $%.4f -> $%.4f to Safety Pocket (now $%.4f); "
                     "$%.4f stays in trading pool (now $%.4f)", pnl, lock, pk["balance"], pnl - lock,
                     st["pool_usd"])
        else:
            st["pool_usd"] += proceeds
            st["consecutive_losses"] += 1
            st["recent_losses"].append(trade)
            st["last_loss_time"] = iso(now)
            log.warning("RULE-TRIGGER | LOSS #%d of %d in a row | cooldown %d min before any new live entry",
                        st["consecutive_losses"], Config.LOSS_STREAK_LIMIT, Config.COOLDOWN_AFTER_LOSS_MIN)
            if st["consecutive_losses"] >= Config.LOSS_STREAK_LIMIT:
                self.trigger_halt(st, now)

    def heartbeat(self, st, now):
        if time.time() - self.last_heartbeat < Config.HEARTBEAT_MIN * 60:
            return
        self.last_heartbeat = time.time()
        pa, pb = st["paper_stats"]["A"], st["paper_stats"]["B"]
        lp = st["live_position"]["symbol"] if st["live_position"] else "none"
        log.info("HEARTBEAT | live=%s halt_until=%s | pool=$%.2f pocket=$%.4f | streak=%d/%d | live_pos=%s | "
                 "live: %d trades pnl $%.4f | paper A: %d trades pnl $%.4f | paper B: %d trades pnl $%.4f | "
                 "trades today %d/%d", Config.LIVE_TRADING, st["halt_until"], st["pool_usd"],
                 st["pocket"]["balance"], st["consecutive_losses"], Config.LOSS_STREAK_LIMIT, lp,
                 st["live_stats"]["n"], st["live_stats"]["pnl"], pa["n"], pa["pnl"], pb["n"], pb["pnl"],
                 st["trades_today"]["count"], Config.MAX_TRADES_PER_DAY)


def cmd_status():
    setup_logging()
    store = StateStore(P("state.json"))
    with store.transaction() as st:
        pk = st["pocket"]
        print(f"Trading pool:      ${st['pool_usd']:.4f}")
        print(f"Safety Pocket:     ${pk['balance']:.4f}  (deposited ${pk['lifetime_deposited']:.4f}, "
              f"emergency-withdrawn ${pk['lifetime_withdrawn']:.4f})")
        print(f"Loss streak:       {st['consecutive_losses']}/{Config.LOSS_STREAK_LIMIT}")
        print(f"Halt until:        {st['halt_until']}  (review acknowledged: {st['review_acknowledged']})")
        print(f"Review file:       {st['review_file']}")
        print(f"Live position:     {st['live_position']['symbol'] if st['live_position'] else 'none'}")
        print(f"Pending order:     {st['pending_order']}")
        print(f"Live stats:        {st['live_stats']}")
        print(f"Paper stats:       {st['paper_stats']}")


def cmd_ack_review(notes):
    setup_logging()
    store = StateStore(P("state.json"))
    with store.transaction() as st:
        if not st["review_required"]:
            sys.exit("No review is pending.")
        if len(notes.strip()) < 40:
            sys.exit("Please write real notes (at least 40 characters): what went wrong, what you'll change.")
        st["review_acknowledged"] = True
        with open(P("lessons.md"), "a") as f:
            f.write(f"\n## {iso(utcnow())}  (review file: {st['review_file']})\n")
            for t in st["recent_losses"]:
                f.write(f"- {t['symbol']} {t['exit_reason']} ${t['net_pnl']:.4f} ctx={json.dumps(t['ctx'])}\n")
            f.write(f"\n{notes.strip()}\n")
        print(f"Review recorded in lessons.md. Live trading resumes after {st['halt_until']}.")
        log.warning("RULE-TRIGGER | REVIEW ACKNOWLEDGED")


def cmd_pocket_withdraw(amount, reason, confirmed):
    setup_logging()
    store = StateStore(P("state.json"))
    with store.transaction() as st:
        pk = st["pocket"]
        limit = Config.EMERGENCY_MAX_PCT * pk["lifetime_deposited"] - pk["lifetime_withdrawn"]
        if not confirmed:
            sys.exit("Add --i-understand to confirm this is a genuine emergency.")
        if len(reason.strip()) < 10:
            sys.exit("Please give a real reason (10+ characters).")
        if amount <= 0 or amount > pk["balance"]:
            sys.exit(f"Amount must be between 0 and the pocket balance (${pk['balance']:.4f}).")
        if amount > limit + 1e-12:
            sys.exit(f"REFUSED: lifetime emergency withdrawals are capped at {Config.EMERGENCY_MAX_PCT:.0%} "
                     f"of everything ever locked. You may still take ${max(limit, 0):.4f}.")
        pk["balance"] -= amount
        pk["lifetime_withdrawn"] += amount
        log.warning("RULE-TRIGGER | EMERGENCY POCKET WITHDRAWAL | $%.4f | reason: %s", amount, reason)
        print(f"Ledger updated: -${amount:.4f}. The bot never moves money; handle the funds yourself on Binance.")


def cmd_clear_pending():
    setup_logging()
    store = StateStore(P("state.json"))
    with store.transaction() as st:
        print(f"Clearing marker: {st['pending_order']}")
        st["pending_order"] = None


def main():
    ap = argparse.ArgumentParser(description="Binance Spot trading co-pilot")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("run")
    sub.add_parser("status")
    sub.add_parser("clear-pending")
    a = sub.add_parser("ack-review")
    a.add_argument("--notes", required=True)
    w = sub.add_parser("pocket-withdraw")
    w.add_argument("--amount", type=float, required=True)
    w.add_argument("--reason", required=True)
    w.add_argument("--i-understand", action="store_true", dest="confirmed")
    args = ap.parse_args()
    if args.cmd in (None, "run"):
        Bot().run()
    elif args.cmd == "status":
        cmd_status()
    elif args.cmd == "ack-review":
        cmd_ack_review(args.notes)
    elif args.cmd == "pocket-withdraw":
        cmd_pocket_withdraw(args.amount, args.reason, args.confirmed)
    elif args.cmd == "clear-pending":
        cmd_clear_pending()


if __name__ == "__main__":
    main()
