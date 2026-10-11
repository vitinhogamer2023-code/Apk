import json
import math
import os
import socket
import subprocess
import shutil
import sqlite3
import threading
import time
import webbrowser
from collections import deque
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

try:
    import websocket  # pacote websocket-client
except ImportError:
    websocket = None

APP_NAME = "BTC Signal Watch PC - V1.8.6 | Horário BR 09-16 + TradingView"
PRODUCT = "BTC-USD"
GRANULARITY = 300  # 5 minutos
TZ = ZoneInfo("America/Sao_Paulo") if ZoneInfo else timezone(timedelta(hours=-3), name="BRT")
PORT = int(os.environ.get("PORT", "8080"))
POLL_SECONDS = 1
LOOKBACK_CANDLES = 48           # 4 horas de velas de 5m para o contexto antes da sessão
ZONE_CANDLES = 20               # zonas recentes ~100 min
MAX_SIGNALS_PER_HOUR = 2  # alvo: até 2 operações por hora; não força entrada sem setup válido
DAILY_SIGNAL_LIMIT = 5
SESSION_START_HOUR = 8
SESSION_END_HOUR = 18
MIN_SCORE = 74
PRE_SIGNAL_MIN_SECONDS = 70     # não manda em cima da hora
PRE_SIGNAL_MAX_SECONDS = 240    # no máximo 4 min antes
SIGNAL_COOLDOWN_SECONDS = 0
POST_TRADE_COOLDOWN_SECONDS = 0  # permite procurar outro sinal assim que a operação termina
CANDIDATE_STABILITY_SECONDS = 30       # setup precisa permanecer válido por pelo menos 30s
CONTEXT_LOCK_SECONDS = 30               # contexto é decidido/travado antes da entrada
ENTRY_EARLY_NOTICE_SECONDS = 30          # aviso antecipado de possível entrada
ENTRY_FINAL_CHECK_SECONDS = 10       # janela desejada para a decisão final
CONTEXT_RISK_GRACE_SECONDS = 12         # evita cancelar por um único "piscar" do feed
NOTICE_SECONDS = 90                     # mantém aviso de cancelamento visível
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals_pc_v1_8_manual.db")

COINBASE_BASE = "https://api.exchange.coinbase.com"
HEADERS = {"Accept": "application/json", "User-Agent": "BTC-Signal-Watch/1.0"}

state_lock = threading.RLock()
ws_candles_lock = threading.RLock()
WS_CANDLES = {}  # candles recentes recebidos ao vivo da Coinbase, indexados por timestamp
WS_LAST_MESSAGE = 0.0
WS_CONNECTED = False
WS_URL = "wss://advanced-trade-ws.coinbase.com"

# O loop de sinais nunca deve ficar bloqueado esperando REST. As consultas REST
# atualizam este cache em uma thread separada; o loop principal lê o cache e
# incorpora os candles que chegam pelo WebSocket a cada segundo.
market_data_lock = threading.RLock()
REST_CANDLE_CACHE = []
REST_PRICE_CACHE = None
REST_LAST_OK = 0.0
MARKET_DATA_ERROR = None
REST_REFRESH_SECONDS = 3

STATE = {
    "price": None,
    "candles": [],
    "analysis": {},
    "pre_signal": None,
    "active": None,
    "test_active": None,
    "last_signal_ts": 0,
    "last_completed_ts": 0,
    "candidate": None,
    "notice": None,
    "last_error": None,
    "last_update": None,
    "last_result_audit": None,
    "feed_ok": False,
    "started_at": datetime.now(TZ).isoformat(),
}


def db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                entry_time TEXT NOT NULL,
                direction TEXT NOT NULL,
                setup TEXT,
                score REAL,
                condition_text TEXT,
                entry_price REAL,
                first_exit_price REAL,
                gale_entry_price REAL,
                gale_exit_price REAL,
                result TEXT,
                used_gale INTEGER DEFAULT 0,
                first_manual_result TEXT,
                gale_manual_result TEXT,
                result_source TEXT DEFAULT 'MANUAL_BROKER',
                notes TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_entry_time ON signals(entry_time)")

        # Migração aditiva: preserva todos os sinais já registrados.
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
        for name, kind in (
            ("entry_stake", "REAL"),
            ("payout_percent", "REAL"),
            ("gale_stake", "REAL"),
        ):
            if name not in columns:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {name} {kind}")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS financial_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                entry_stake REAL NOT NULL DEFAULT 6.0,
                payout_percent REAL NOT NULL DEFAULT 85.0,
                updated_at TEXT
            )
        """)
        conn.execute("""
            INSERT OR IGNORE INTO financial_settings
                (id, entry_stake, payout_percent, updated_at)
            VALUES (1, 6.0, 85.0, ?)
        """, (datetime.now(TZ).isoformat(),))
        conn.commit()


def get_financial_settings():
    with db_conn() as conn:
        row = conn.execute(
            "SELECT entry_stake, payout_percent FROM financial_settings WHERE id=1"
        ).fetchone()
    if not row:
        return {"entry_stake": 6.0, "payout_percent": 85.0}
    return {
        "entry_stake": float(row["entry_stake"]),
        "payout_percent": float(row["payout_percent"]),
    }


def save_financial_settings(entry_stake, payout_percent):
    entry_stake = float(entry_stake)
    payout_percent = float(payout_percent)
    if not 0.01 <= entry_stake <= 1000000:
        raise ValueError("A entrada deve estar entre R$ 0,01 e R$ 1.000.000.")
    if not 1 <= payout_percent <= 100:
        raise ValueError("O payout deve estar entre 1% e 100%.")
    with db_conn() as conn:
        conn.execute("""
            UPDATE financial_settings
            SET entry_stake=?, payout_percent=?, updated_at=?
            WHERE id=1
        """, (round(entry_stake, 2), round(payout_percent, 2), datetime.now(TZ).isoformat()))
        conn.commit()
    return get_financial_settings()




def now_local():
    return datetime.now(TZ)


def iso_local(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ).isoformat()


def parse_iso(s):
    return datetime.fromisoformat(s).astimezone(TZ)


def day_bounds(dt=None):
    dt = dt or now_local()
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def today_stats():
    start, end = day_bounds()
    with db_conn() as conn:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) AS losses,
                COUNT(*) AS total
            FROM signals
            WHERE entry_time >= ? AND entry_time < ? AND result IN ('WIN','LOSS')
            """,
            (iso_local(start), iso_local(end)),
        ).fetchone()
    return {
        "wins": int(row["wins"] or 0),
        "losses": int(row["losses"] or 0),
        "total": int(row["total"] or 0),
    }


def recent_results(limit=30):
    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, entry_time, direction, setup, score, result, used_gale,
                   entry_price, first_exit_price, gale_entry_price, gale_exit_price,
                   first_manual_result, gale_manual_result, result_source
            FROM signals
            WHERE result IN ('WIN','LOSS')
            ORDER BY id DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            dt = parse_iso(d["entry_time"])
            d["time"] = dt.strftime("%H:%M")
            d["date"] = dt.strftime("%d/%m")
        except Exception:
            d["time"] = "--:--"
            d["date"] = "--/--"
        d["used_gale"] = bool(d["used_gale"])
        out.append(d)
    return out




def init_test_db():
    """Tabela separada para o modelo de teste: entrada somente na segunda oportunidade."""
    with db_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS test_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                signal_time TEXT NOT NULL,
                real_entry_time TEXT,
                direction TEXT NOT NULL,
                result TEXT,
                used_gale INTEGER NOT NULL DEFAULT 0,
                notes TEXT
            )
            """
        )
        conn.commit()


def daily_financial_summary(limit=90):
    """Resumo financeiro diário; preserva a regra dos registros antigos."""
    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT entry_time, result, used_gale,
                   first_manual_result, gale_manual_result,
                   entry_stake, payout_percent, gale_stake
            FROM signals
            WHERE result IN ('WIN','LOSS')
            ORDER BY entry_time DESC
            LIMIT ?
            """,
            (limit * 10,),
        ).fetchall()

    days = {}

    for r in rows:
        try:
            dt = parse_iso(r["entry_time"])
            key = dt.strftime("%Y-%m-%d")
        except Exception:
            continue

        if key not in days:
            days[key] = {
                "date": key,
                "total": 0,
                "wins": 0,
                "losses": 0,
                "direct_wins": 0,
                "gale_wins": 0,
                "direct_losses": 0,
                "gale_losses": 0,
                "gales": 0,
                "result": 0.0,
            }

        d = days[key]
        d["total"] += 1

        result = str(r["result"] or "").upper()
        used_gale = bool(r["used_gale"])

        if used_gale:
            d["gales"] += 1

        # Sinais novos usam os valores registrados na própria operação.
        # Sinais antigos sem esses campos mantêm o cálculo histórico anterior.
        has_financial_data = (
            r["entry_stake"] is not None
            and r["payout_percent"] is not None
        )

        if has_financial_data:
            stake = float(r["entry_stake"])
            payout = float(r["payout_percent"]) / 100.0
            gale_stake = (
                float(r["gale_stake"])
                if r["gale_stake"] is not None
                else stake * 2
            )

            if result == "WIN":
                if used_gale:
                    profit = -stake + gale_stake * payout
                else:
                    profit = stake * payout
            else:
                if used_gale:
                    profit = -stake - gale_stake
                else:
                    profit = -stake
        else:
            if result == "WIN":
                profit = 3.50 if used_gale else 4.25
            else:
                profit = -15.00 if used_gale else -5.00

        d["result"] += profit

        if result == "WIN":
            d["wins"] += 1
            if used_gale:
                d["gale_wins"] += 1
            else:
                d["direct_wins"] += 1
        elif result == "LOSS":
            d["losses"] += 1
            if used_gale:
                d["gale_losses"] += 1
            else:
                d["direct_losses"] += 1

    out = list(days.values())
    for d in out:
        d["result"] = round(d["result"], 2)
        try:
            d["date_br"] = datetime.fromisoformat(d["date"]).strftime("%d/%m/%Y")
        except Exception:
            d["date_br"] = d["date"]

    out.sort(key=lambda x: x["date"], reverse=True)

    accumulated = 0.0
    for d in reversed(out):
        accumulated += d["result"]
        d["accumulated"] = round(accumulated, 2)

    out.reverse()
    out.reverse()

    return out[:limit]


def count_signals_this_hour(dt=None):
    dt = dt or now_local()
    start = dt.replace(minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=1)
    with db_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM signals WHERE entry_time >= ? AND entry_time < ?",
            (iso_local(start), iso_local(end)),
        ).fetchone()
    return int(row["n"] or 0)


def count_operations_today(dt=None):
    """Conta sinais principais criados hoje; o Gale não entra como nova operação."""
    start, end = day_bounds(dt)
    with db_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM signals WHERE entry_time >= ? AND entry_time < ?",
            (iso_local(start), iso_local(end)),
        ).fetchone()
    return int(row["n"] or 0)


def session_status():
    """Analisa o mercado 24h; libera operações das 08:00 às 18:00."""
    now = now_local()
    if count_operations_today() >= DAILY_SIGNAL_LIMIT:
        return "LIMITE_ATINGIDO"
    if now.hour < SESSION_START_HOUR or now.hour >= SESSION_END_HOUR:
        return "OBSERVACAO"
    return "OPERACIONAL"

def can_start_new_operation(dt=None):
    dt = dt or now_local()
    return count_operations_today(dt) < DAILY_SIGNAL_LIMIT


def http_json(url, params=None, timeout=10):
    if params:
        url = url + "?" + urlencode(params)
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _store_ws_candle(item):
    """Normaliza candle do canal público Coinbase Advanced Trade."""
    global WS_LAST_MESSAGE
    try:
        raw_start = item["start"]
        if isinstance(raw_start, (int, float)) or str(raw_start).strip().isdigit():
            ts = int(float(raw_start))
        else:
            start_text = str(raw_start).strip()
            if start_text.endswith("Z"):
                start_text = start_text[:-1] + "+00:00"
            parsed_start = datetime.fromisoformat(start_text)
            if parsed_start.tzinfo is None:
                parsed_start = parsed_start.replace(tzinfo=timezone.utc)
            ts = int(parsed_start.timestamp())
        candle = {
            "ts": ts,
            "time": datetime.fromtimestamp(ts, timezone.utc).astimezone(TZ).isoformat(),
            "open": float(item["open"]),
            "high": float(item["high"]),
            "low": float(item["low"]),
            "close": float(item["close"]),
            "volume": float(item.get("volume", 0) or 0),
        }
    except (KeyError, TypeError, ValueError):
        return
    with ws_candles_lock:
        WS_CANDLES[ts] = candle
        # Mantém uma janela recente e limita a memória.
        cutoff = ts - (GRANULARITY * 30)
        for old_ts in list(WS_CANDLES):
            if old_ts < cutoff:
                WS_CANDLES.pop(old_ts, None)
        WS_LAST_MESSAGE = time.time()


def websocket_candle_loop():
    """Mantém candles recentes atualizados sem depender do atraso do endpoint REST."""
    global WS_CONNECTED, WS_LAST_MESSAGE
    if websocket is None:
        print("AVISO: websocket-client não instalado; usando apenas REST.")
        return
    delay = 1
    while True:
        ws = None
        try:
            ws = websocket.create_connection(WS_URL, timeout=12, enable_multithread=True)
            ws.settimeout(15)
            ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": [PRODUCT],
                "channel": "candles",
            }))
            with ws_candles_lock:
                WS_CONNECTED = True
            delay = 1
            while True:
                raw = ws.recv()
                if not raw:
                    continue
                msg = json.loads(raw)
                if msg.get("channel") != "candles":
                    continue
                for event in msg.get("events", []):
                    for item in event.get("candles", []):
                        if item.get("product_id", PRODUCT) == PRODUCT:
                            _store_ws_candle(item)
        except Exception as e:
            with ws_candles_lock:
                WS_CONNECTED = False
            print(f"WebSocket Coinbase desconectado; reconectando: {str(e)[:120]}")
            time.sleep(delay)
            delay = min(delay * 2, 15)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass


def merge_ws_candles(rest_candles):
    """Substitui candles REST defasados pelos valores mais recentes do WebSocket."""
    merged = {int(c["ts"]): c for c in rest_candles}
    with ws_candles_lock:
        live = list(WS_CANDLES.values())
    for c in live:
        merged[int(c["ts"])] = c
    return [merged[k] for k in sorted(merged)][-120:]


def fetch_candles():
    url = f"{COINBASE_BASE}/products/{PRODUCT}/candles"
    raw = http_json(url, {"granularity": GRANULARITY}, timeout=10)
    if not isinstance(raw, list):
        raise RuntimeError(f"Resposta inesperada de candles: {raw}")
    candles = []
    for item in raw:
        if not isinstance(item, list) or len(item) < 6:
            continue
        ts, low, high, opn, close, vol = item[:6]
        candles.append({
            "ts": int(ts),
            "time": datetime.fromtimestamp(int(ts), timezone.utc).astimezone(TZ).isoformat(),
            "open": float(opn),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(vol),
        })
    candles.sort(key=lambda x: x["ts"])
    # A API REST pode demorar a publicar a vela recém-iniciada. O WebSocket
    # atualiza as velas recentes e tem prioridade para timestamps iguais.
    return merge_ws_candles(candles)


def fetch_ticker():
    url = f"{COINBASE_BASE}/products/{PRODUCT}/ticker"
    data = http_json(url, timeout=8)
    return float(data["price"])


def ema(values, period):
    if not values:
        return None
    k = 2 / (period + 1)
    result = values[0]
    for v in values[1:]:
        result = v * k + result * (1 - k)
    return result


def rsi(values, period=14):
    if len(values) < period + 1:
        return 50.0
    gains = []
    losses = []
    for a, b in zip(values[-period-1:-1], values[-period:]):
        diff = b - a
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles, period=14):
    if len(candles) < 2:
        return 0.0
    trs = []
    for prev, cur in zip(candles[-period-1:-1], candles[-period:]):
        tr = max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev["close"]),
            abs(cur["low"] - prev["close"]),
        )
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0.0


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def current_bucket_start(now=None):
    now = now or now_local()
    minute = (now.minute // 5) * 5
    return now.replace(minute=minute, second=0, microsecond=0)


def next_bucket_start(now=None):
    return current_bucket_start(now) + timedelta(minutes=5)


def candle_for_ts(candles, dt):
    target = int(dt.astimezone(timezone.utc).timestamp())
    for c in candles:
        if c["ts"] == target:
            return c
    return None


def analyze_market(candles, live_price=None):
    if len(candles) < 24:
        return {"ready": False, "reason": "Aguardando candles suficientes", "score": 0}

    now = now_local()
    bucket_ts = int(current_bucket_start(now).astimezone(timezone.utc).timestamp())
    closed = [c for c in candles if c["ts"] < bucket_ts]
    current = next((c for c in candles if c["ts"] == bucket_ts), None)
    recent = closed[-LOOKBACK_CANDLES:]
    if len(recent) < 20:
        return {"ready": False, "reason": "Aguardando janela recente", "score": 0}

    closes = [c["close"] for c in recent]
    price = float(live_price or (current["close"] if current else closes[-1]))
    e9 = ema(closes[-24:], 9)
    e21 = ema(closes[-36:], 21)
    rv = rsi(closes, 14)
    av = atr(recent, 14)
    av = av if av > 0 else max(price * 0.001, 1)

    zone = recent[-ZONE_CANDLES:]
    highs = sorted(c["high"] for c in zone)
    lows = sorted(c["low"] for c in zone)
    # Evita uma única sombra extrema dominar a zona.
    resistance = sum(highs[-4:]) / min(4, len(highs))
    support = sum(lows[:4]) / min(4, len(lows))
    range_width = max(resistance - support, av)
    pos = clamp((price - support) / range_width, 0, 1)
    ema_gap_atr = abs(e9 - e21) / av
    slope = (ema(closes[-8:], 5) - ema(closes[-16:-8], 5)) / av if len(closes) >= 16 else 0

    trend = "LATERAL"
    if e9 > e21 and slope > 0.10:
        trend = "ALTA"
    elif e9 < e21 and slope < -0.10:
        trend = "BAIXA"

    sideways = ema_gap_atr < 0.55 and range_width / av < 6.2
    near_support = (price - support) <= av * 0.85
    near_resistance = (resistance - price) <= av * 0.85

    last3 = recent[-3:]
    up_count = sum(1 for c in last3 if c["close"] > c["open"])
    down_count = sum(1 for c in last3 if c["close"] < c["open"])
    last = recent[-1]
    consecutive_call = 0
    consecutive_put = 0
    for c in reversed(recent):
        if c["close"] > c["open"]:
            consecutive_call += 1
        else:
            break
    for c in reversed(recent):
        if c["close"] < c["open"]:
            consecutive_put += 1
        else:
            break
    body = abs(last["close"] - last["open"])
    lower_wick = min(last["open"], last["close"]) - last["low"]
    upper_wick = last["high"] - max(last["open"], last["close"])
    lower_reject = lower_wick > max(body * 0.7, av * 0.12)
    upper_reject = upper_wick > max(body * 0.7, av * 0.12)

    setup = "AGUARDAR"
    direction = None
    condition = None
    score = 0.0
    reasons = []

    # 1) Lateralização: operar bordas recentes, nunca o meio da faixa.
    if sideways and near_support and pos < 0.30:
        setup = "LATERALIZACAO_SUPORTE"
        direction = "CALL"
        score = 58
        score += 9 if rv < 46 else 3 if rv < 52 else 0
        score += 8 if lower_reject else 0
        score += 5 if down_count >= 2 else 0
        score += 6 if pos < 0.18 else 2
        condition = "Entrar somente se a vela atual fechar PUT (vermelha) E mostrar rejeição forte inferior no suporte."
        reasons = ["lateralização recente", "preço perto do suporte", "busca reação compradora"]
    elif sideways and near_resistance and pos > 0.70:
        setup = "LATERALIZACAO_RESISTENCIA"
        direction = "PUT"
        score = 58
        score += 9 if rv > 54 else 3 if rv > 48 else 0
        score += 8 if upper_reject else 0
        score += 5 if up_count >= 2 else 0
        score += 6 if pos > 0.82 else 2
        condition = "Entrar somente se a vela atual fechar CALL (verde) E mostrar rejeição forte superior na resistência."
        reasons = ["lateralização recente", "preço perto da resistência", "busca reação vendedora"]

    # 2) Pullback em tendência recente.
    if setup == "AGUARDAR" and trend == "ALTA":
        dist_ema = abs(price - e9) / av
        pullback = price <= e9 + av * 0.35 and price >= e21 - av * 0.45
        if pullback:
            setup = "PULLBACK_ALTA"
            direction = "CALL"
            score = 60 + clamp((e9 - e21) / av * 8, 0, 12)
            score += 7 if rv < 58 else 2
            score += 6 if down_count >= 2 else 0
            score += 7 if near_support or lower_reject else 0
            score += 4 if dist_ema < 0.55 else 0
            condition = "Entrar somente se a vela atual fechar PUT (vermelha) E mostrar rejeição forte inferior; é a confirmação do recuo."
            reasons = ["tendência curta de alta", "recuo para a região das médias", "pullback recente"]
    elif setup == "AGUARDAR" and trend == "BAIXA":
        dist_ema = abs(price - e9) / av
        pullback = price >= e9 - av * 0.35 and price <= e21 + av * 0.45
        if pullback:
            setup = "PULLBACK_BAIXA"
            direction = "PUT"
            score = 60 + clamp((e21 - e9) / av * 8, 0, 12)
            score += 7 if rv > 42 else 2
            score += 6 if up_count >= 2 else 0
            score += 7 if near_resistance or upper_reject else 0
            score += 4 if dist_ema < 0.55 else 0
            condition = "Entrar somente se a vela atual fechar CALL (verde) E mostrar rejeição forte superior; é a confirmação do repique."
            reasons = ["tendência curta de baixa", "repique para a região das médias", "pullback recente"]

    # 3) Reteste após rompimento recente (últimas poucas velas).
    prev_zone = recent[-12:-2]
    if setup == "AGUARDAR" and len(prev_zone) >= 8:
        prev_res = max(c["high"] for c in prev_zone)
        prev_sup = min(c["low"] for c in prev_zone)
        breakout_up = any(c["close"] > prev_res + av * 0.10 for c in recent[-2:])
        breakout_dn = any(c["close"] < prev_sup - av * 0.10 for c in recent[-2:])
        if breakout_up and abs(price - prev_res) <= av * 0.75:
            setup = "RETESTE_ALTA"
            direction = "CALL"
            score = 66 + (8 if e9 >= e21 else 0) + (5 if rv >= 50 else 0)
            condition = "Entrar somente se a vela atual fechar CALL (verde) E mostrar rejeição forte inferior no reteste."
            reasons = ["rompimento recente", "reteste da antiga resistência", "continuação somente com confirmação"]
        elif breakout_dn and abs(price - prev_sup) <= av * 0.75:
            setup = "RETESTE_BAIXA"
            direction = "PUT"
            score = 66 + (8 if e9 <= e21 else 0) + (5 if rv <= 50 else 0)
            condition = "Entrar somente se a vela atual fechar PUT (vermelha) E mostrar rejeição forte superior no reteste."
            reasons = ["rompimento recente", "reteste do antigo suporte", "continuação somente com confirmação"]

    score = round(clamp(score, 0, 95), 1)
    if setup == "AGUARDAR":
        reasons = ["nenhum setup forte nas velas recentes"]

    return {
        "ready": True,
        "price": price,
        "ema9": round(e9, 2),
        "ema21": round(e21, 2),
        "rsi": round(rv, 1),
        "atr": round(av, 2),
        "support": round(support, 2),
        "resistance": round(resistance, 2),
        "position_in_range": round(pos * 100, 1),
        "trend": trend,
        "sideways": sideways,
        "near_support": near_support,
        "near_resistance": near_resistance,
        "consecutive_call": consecutive_call,
        "consecutive_put": consecutive_put,
        "setup": setup,
        "direction": direction,
        "condition": condition,
        "score": score,
        "reasons": reasons,
        "window_candles": len(recent),
        "current_candle": current,
    }


def confirmation_status(pre, candle):
    """Retorna a confirmação final exigida pelo texto do pré-sinal.

    A cor e a rejeição são obrigatórias JUNTAS. Durante a formação da vela
    estes valores servem apenas como prévia; no fechamento viram a regra final.
    """
    setup = pre.get("setup", "")
    direction = pre.get("direction")
    expected = None
    if setup in ("PULLBACK_ALTA", "LATERALIZACAO_SUPORTE"):
        expected = "PUT"
    elif setup in ("PULLBACK_BAIXA", "LATERALIZACAO_RESISTENCIA"):
        expected = "CALL"
    elif setup == "RETESTE_ALTA":
        expected = "PUT"
    elif setup == "RETESTE_BAIXA":
        expected = "CALL"

    if not candle or expected is None:
        return {
            "met": False, "expected_color": expected or "—", "actual_color": "—",
            "color_ok": False, "rejection_ok": False,
            "rejection_label": "rejeição forte"
        }

    opn = float(candle["open"])
    close = float(candle["close"])
    high = float(candle["high"])
    low = float(candle["low"])
    body = abs(close - opn)
    actual = "CALL" if close > opn else "PUT" if close < opn else "DOJI"
    atr_ref = float(pre.get("atr") or STATE.get("analysis", {}).get("atr") or max(abs(close) * 0.001, 1))
    wick_floor = max(body * 0.70, atr_ref * 0.12)
    lower_wick = min(opn, close) - low
    upper_wick = high - max(opn, close)

    # CALL procura rejeição compradora (pavio inferior); PUT procura rejeição
    # vendedora (pavio superior), independentemente da cor exigida pelo setup.
    if direction == "CALL":
        rejection_ok = lower_wick >= wick_floor
        rejection_label = "rejeição forte inferior"
    else:
        rejection_ok = upper_wick >= wick_floor
        rejection_label = "rejeição forte superior"

    color_ok = actual == expected
    return {
        "met": bool(color_ok and rejection_ok),
        "expected_color": expected,
        "actual_color": actual,
        "color_ok": bool(color_ok),
        "rejection_ok": bool(rejection_ok),
        "rejection_label": rejection_label,
        "lower_wick": round(lower_wick, 2),
        "upper_wick": round(upper_wick, 2),
        "body": round(body, 2),
    }


def current_candle_condition_met(pre, current_candle):
    return confirmation_status(pre, current_candle)["met"]


def pre_context_status(pre, analysis):
    """Revalida a estrutura enquanto ainda há tempo de avisar o usuário.

    Depois de CONTEXT_LOCK_SECONDS antes da entrada, este contexto deixa de ser
    reavaliado. Assim o app não muda de ideia no segundo 00.
    """
    if not analysis or not analysis.get("ready"):
        return False, "Leitura de mercado temporariamente indisponível."
    if analysis.get("direction") != pre.get("direction"):
        return False, "A direção da leitura mudou antes da entrada."
    if analysis.get("setup") != pre.get("setup"):
        return False, "O setup saiu da zona/estrutura que originou o pré-sinal."
    if analysis.get("score", 0) < (MIN_SCORE - 2):
        return False, f'A força do setup caiu para {analysis.get("score", 0):.0f}/100.'
    return True, "Contexto e zona continuam válidos."


def set_notice(title, message, kind="info", notify_android=False):
    STATE["notice"] = {
        "title": title,
        "message": message,
        "kind": kind,
        "created_at": iso_local(now_local()),
        "expires_ts": time.time() + NOTICE_SECONDS,
    }

    # Por padrão, avisos permanecem somente na interface. A notificação nativa
    # é enviada apenas quando explicitamente solicitada (preparação aos 10s).
    if not notify_android:
        return
    key = (str(title), str(message))
    last = STATE.get("_last_android_notice")
    now_ts = time.time()
    if key != last or now_ts - STATE.get("_last_android_notice_ts", 0) > NOTICE_SECONDS:
        cmd = shutil.which("termux-notification")
        if cmd:
            try:
                subprocess.run(
                    [cmd, "--id", "btc-signal-watch", "--title", str(title),
                     "--content", str(message), "--priority", "high",
                     "--sound", "--vibrate", "200,100,200"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=3,
                    check=False,
                )
                STATE["_last_android_notice"] = key
                STATE["_last_android_notice_ts"] = now_ts
            except Exception:
                pass


def clear_expired_notice():
    n = STATE.get("notice")
    if n and time.time() > float(n.get("expires_ts", 0)):
        STATE["notice"] = None

def save_signal(pre, entry_price=None):
    settings = get_financial_settings()
    entry_stake = float(settings["entry_stake"])
    payout_percent = float(settings["payout_percent"])
    gale_stake = round(entry_stake * 2, 2)

    with db_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO signals(
                created_at, entry_time, direction, setup, score,
                condition_text, entry_price, result, used_gale, notes,
                entry_stake, payout_percent, gale_stake
            )
            VALUES(?,?,?,?,?,?,?,?,0,?,?,?,?)
            """,
            (
                pre["created_at"], pre["entry_time"], pre["direction"], pre["setup"],
                pre["score"], pre["condition"], entry_price, "OPEN",
                json.dumps(pre.get("reasons", []), ensure_ascii=False),
                entry_stake, payout_percent, gale_stake
            ),
        )
        conn.commit()
        return cur.lastrowid


def update_signal(signal_id, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    sql = "UPDATE signals SET " + ", ".join(f"{k}=?" for k in keys) + " WHERE id=?"
    vals = [fields[k] for k in keys] + [signal_id]
    with db_conn() as conn:
        conn.execute(sql, vals)
        conn.commit()


def maybe_create_pre_signal(analysis):
    now = now_local()
    if session_status() != "OPERACIONAL":
        STATE["candidate"] = None
        return
    if STATE.get("pre_signal") or STATE.get("active"):
        STATE["candidate"] = None
        return
    if analysis.get("score", 0) < MIN_SCORE or not analysis.get("direction"):
        STATE["candidate"] = None
        return

    # FILTRO DE DIREÇÃO POR MÉDIAS + PROTEÇÃO CONTRA SEQUÊNCIA ESTICADA.
    direction = analysis.get("direction")
    price = analysis.get("price")
    ema9 = analysis.get("ema9")
    ema21 = analysis.get("ema21")
    consecutive_call = analysis.get("consecutive_call", 0)
    consecutive_put = analysis.get("consecutive_put", 0)

    if price is not None and ema9 is not None and ema21 is not None:
        if price > max(ema9, ema21) and direction != "PUT":
            print(f"🚫 BLOQUEADO — EMA: preço acima das duas médias, direção {direction}")
            STATE["candidate"] = None
            return
        if price < min(ema9, ema21) and direction != "CALL":
            print(f"🚫 BLOQUEADO — EMA: preço abaixo das duas médias, direção {direction}")
            STATE["candidate"] = None
            return

    if direction == "CALL" and consecutive_call >= 7:
        print(f"🚫 BLOQUEADO — SEQUÊNCIA: {consecutive_call} CALL seguidas")
        STATE["candidate"] = None
        return
    if direction == "PUT" and consecutive_put >= 7:
        print(f"🚫 BLOQUEADO — SEQUÊNCIA: {consecutive_put} PUT seguidas")
        STATE["candidate"] = None
        return
    if count_signals_this_hour(now) >= MAX_SIGNALS_PER_HOUR:
        STATE["candidate"] = None
        return
    if count_operations_today(now) >= DAILY_SIGNAL_LIMIT:
        STATE["candidate"] = None
        return
    if time.time() - STATE.get("last_signal_ts", 0) < SIGNAL_COOLDOWN_SECONDS:
        return
    if time.time() - STATE.get("last_completed_ts", 0) < POST_TRADE_COOLDOWN_SECONDS:
        return

    # Não transforma uma leitura momentânea em sinal. O mesmo setup/direção
    # precisa continuar válido por alguns polls antes do pré-sinal.
    key = f'{analysis.get("setup")}:{analysis.get("direction")}'
    cand = STATE.get("candidate")
    now_ts = time.time()
    if not cand or cand.get("key") != key:
        STATE["candidate"] = {"key": key, "first_seen": now_ts}
        return
    if now_ts - cand.get("first_seen", now_ts) < CANDIDATE_STABILITY_SECONDS:
        return

    entry = next_bucket_start(now)
    seconds = (entry - now).total_seconds()
    if seconds < PRE_SIGNAL_MIN_SECONDS or seconds > PRE_SIGNAL_MAX_SECONDS:
        return

    STATE["pre_signal"] = {
        "created_at": iso_local(now),
        "entry_time": iso_local(entry),
        "direction": analysis["direction"],
        "setup": analysis["setup"],
        "score": analysis["score"],
        "condition": analysis["condition"],
        "reasons": analysis.get("reasons", []),
        "support": analysis.get("support"),
        "resistance": analysis.get("resistance"),
        "atr": analysis.get("atr"),
        "status": "PRE_SINAL",
        "context_locked": False,
        "risk_since": None,
        "risk_reason": None,
        "live_confirmation": {},
    }
    STATE["candidate"] = None


def process_pre_signal(candles):
    pre = STATE.get("pre_signal")
    if not pre:
        return

    now = now_local()
    entry = parse_iso(pre["entry_time"])
    seconds_left = (entry - now).total_seconds()
    signal_candle_start = entry - timedelta(minutes=5)
    confirmation_candle = candle_for_ts(candles, signal_candle_start)

    # Mostra ao vivo a cor/rejeição que estão se formando. Antes do fechamento
    # isto é apenas informativo; a confirmação oficial ocorre na borda da vela.
    pre["live_confirmation"] = confirmation_status(pre, confirmation_candle)

    # FASE 1 — o pré-sinal já foi encontrado e fica protegido contra
    # oscilações momentâneas de score/setup. A confirmação oficial é que
    # decide a entrada.
    if now < entry and not pre.get("context_locked"):
        pre["risk_since"] = None
        pre["risk_reason"] = None
        pre["status"] = "PRE_SINAL"

        # 30s antes da entrada, trava o contexto original.
        if seconds_left <= CONTEXT_LOCK_SECONDS:
            pre["context_locked"] = True
            pre["locked_at"] = iso_local(now)
            pre["status"] = "TRAVADO"

        STATE["pre_signal"] = pre
        return

    # AVISO ANTECIPADO: apenas prepara o usuário; não aprova a vela
    # nem inicia/descarta operação com base em uma vela ainda aberta.
    if now < entry:
        if seconds_left <= ENTRY_EARLY_NOTICE_SECONDS and not pre.get("early_notice"):
            pre["early_notice"] = True
            pre["status"] = "AGUARDANDO_FECHAMENTO"
            set_notice(
                "POSSÍVEL ENTRADA — PREPARE-SE",
                f'{pre.get("direction")} previsto para {entry.strftime("%H:%M")}. Ainda depende da confirmação da vela fechada; não opere apenas por este aviso.',
                "info",
                notify_android=True,
            )
        elif seconds_left > ENTRY_EARLY_NOTICE_SECONDS:
            pre["status"] = "TRAVADO"
            pre["early_notice"] = False

        # Aviso principal aos 10s somente se os critérios estiverem presentes.
        if seconds_left <= ENTRY_FINAL_CHECK_SECONDS and not pre.get("final_check_notice"):
            live_conf = pre.get("live_confirmation") or {}
            if live_conf.get("met"):
                pre["final_check_notice"] = True
                pre["armed_confirmation"] = True
                set_notice(
                    f'SINAL ENCONTRADO — {pre.get("direction")}',
                    f'Rejeição forte identificada. Só entre se a vela fechar em {live_conf.get("expected_color", "—")}.',
                    "info",
                    notify_android=True,
                )

        STATE["pre_signal"] = pre
        return

    # Se o app reiniciou/ficou sem feed e chegou à entrada sem ter conseguido
    # travar o contexto 30s antes, ele NÃO toma uma decisão atrasada.
    if not pre.get("context_locked"):
        set_notice(
            "SINAL CANCELADO",
            f'{pre.get("direction")} {entry.strftime("%H:%M")} — o contexto não pôde ser travado com {CONTEXT_LOCK_SECONDS}s de antecedência. NÃO OPERAR.',
            "cancel",
        )
        STATE["pre_signal"] = None
        return

    # CONFIRMAÇÃO FINAL: só avalia a vela anterior quando o horário
    # da próxima vela já começou. A vela precisa ter timestamp anterior
    # ao bucket atual; nunca aprovar a vela ainda em formação.
    current_bucket = current_bucket_start(now)
    c = confirmation_candle
    if not c or int(c.get("ts", 0)) >= int(current_bucket.timestamp()):
        STATE["pre_signal"] = pre
        return

    conf = confirmation_status(pre, c)
    if not pre.get("armed_confirmation"):
        set_notice(
            "SINAL NÃO CONFIRMADO",
            "Critérios não confirmados na janela dos 10 segundos. Sem operação; voltando à análise.",
            "cancel",
        )
        STATE["pre_signal"] = None
        return
    pre["live_confirmation"] = conf
    pre["confirmation_checked_at"] = iso_local(now)
    if not conf["met"]:
        set_notice(
            "SINAL NÃO CONFIRMADO",
            "A vela fechou sem cumprir a cor exigida e a rejeição forte. "
            "Sem operação; voltando à análise normal.",
            "cancel",
        )
        STATE["pre_signal"] = None
        return

    if session_status() != "OPERACIONAL":
        set_notice("SINAL CANCELADO", "A sessão operacional foi encerrada. NÃO OPERAR.", "cancel")
        STATE["pre_signal"] = None
        return

    # Não apagar o sinal aprovado por atraso na publicação do candle.
    pre["confirmation_approved"] = True
    entry_candle = candle_for_ts(candles, entry)
    if not entry_candle:
        atraso = max(0.0, (now - entry).total_seconds())
        if not pre.get("entry_sync_waiting"):
            set_notice(
                "CONFIRMAÇÃO APROVADA — SINCRONIZANDO",
                f'{pre.get("direction")} {entry.strftime("%H:%M")} — '
                'cor e rejeição confirmadas; aguardando o feed publicar '
                'o candle de entrada. Confirmação preservada. '
                'Confira o preço atual antes de operar.',
                "info",
            )
            pre["entry_sync_waiting"] = True
        pre["entry_sync_delay_seconds"] = round(atraso, 1)
        STATE["pre_signal"] = pre
        return
    pre.pop("entry_sync_waiting", None)
    entry_price = entry_candle["open"]
    pre["confirmation_approved"] = True
    signal_id = save_signal(pre, entry_price)
    STATE["active"] = {
        "id": signal_id,
        "entry_time": pre["entry_time"],
        "expiry_time": iso_local(entry + timedelta(minutes=5)),
        "direction": pre["direction"],
        "setup": pre["setup"],
        "score": pre["score"],
        "entry_price": entry_price,
        "stage": "ENTRADA",
        "used_gale": False,
        "status": "EM_ANDAMENTO",
    }
    # MODELO DE TESTE:
    # a primeira entrada é apenas observada; só vira operação real
    # se essa primeira oportunidade terminar em LOSS.
    STATE["test_active"] = {
        "signal_time": pre["entry_time"],
        "direction": pre["direction"],
        "stage": "OBSERVACAO",
        "start_time": pre["entry_time"],
        "expiry_time": iso_local(entry + timedelta(minutes=5)),
        "real_entry_time": None,
        "used_gale": False,
        "status": "OBSERVANDO",
    }
    STATE["pre_signal"] = None
    STATE["last_signal_ts"] = time.time()
    STATE["notice"] = None

def candle_color(candle):
    """Cor objetiva da própria vela usada na apuração."""
    if not candle:
        return None
    opn = float(candle["open"])
    close = float(candle["close"])
    if close > opn:
        return "CALL"
    if close < opn:
        return "PUT"
    return "DOJI"


def direction_win(direction, entry_price, exit_price):
    """Compatibilidade: WIN só quando a direção bate com OPEN x CLOSE."""
    if entry_price is None or exit_price is None:
        return None
    color = "CALL" if exit_price > entry_price else "PUT" if exit_price < entry_price else "DOJI"
    return color == direction


def boundary_prices(candles, start, expiry):
    """
    Apura o resultado pela PRÓPRIA vela da operação na Coinbase BTC-USD:
      entrada = OPEN da vela que começa em `start`
      saída   = CLOSE dessa mesma vela de 5 minutos

    Isto evita uma divergência que podia acontecer ao usar o OPEN da vela seguinte:
    em mercado contínuo pode existir um pequeno gap entre o último negócio da vela
    encerrada e o primeiro negócio da próxima vela. A cor visual da vela é definida
    por OPEN x CLOSE da própria vela, então WIN/LOSS agora segue exatamente isso.
    """
    c = candle_for_ts(candles, start)
    if not c:
        return None, None, None
    entry_price = float(c["open"])
    exit_price = float(c["close"])
    return entry_price, exit_price, c


def submit_test_result(result):
    """Registra o resultado do modelo de teste, separado do modelo original."""
    result = str(result or "").upper()
    if result not in ("WIN", "LOSS"):
        return False, "Resultado inválido."

    test_active = STATE.get("test_active")
    if not test_active:
        return False, "Não há operação do modelo de teste aguardando resultado."

    if test_active.get("status") != "AGUARDANDO_RESULTADO":
        return False, "O modelo de teste ainda não está aguardando resultado."

    expiry = parse_iso(test_active["expiry_time"])
    stage = test_active.get("stage")

    # A primeira operação é apenas observada e nunca é registrada no histórico.
    if stage == "OBSERVACAO":
        if result == "WIN":
            STATE["test_active"] = None
            return True, "Primeira oportunidade WIN. Modelo de teste cancelado sem registrar operação real."

        # LOSS: a próxima vela vira a operação REAL.
        real_entry = expiry
        STATE["test_active"] = {
            "signal_time": test_active["signal_time"],
            "direction": test_active["direction"],
            "stage": "REAL",
            "start_time": iso_local(real_entry),
            "expiry_time": iso_local(real_entry + timedelta(minutes=5)),
            "real_entry_time": iso_local(real_entry),
            "used_gale": False,
            "status": "EM_ANDAMENTO",
        }
        return True, "Primeira oportunidade LOSS. Operação REAL do teste iniciada."

    # Resultado da operação REAL.
    if stage == "REAL":
        if result == "WIN":
            save_test_signal(
                test_active["signal_time"],
                test_active["real_entry_time"],
                test_active["direction"],
                "WIN",
                used_gale=False,
                notes="WIN na primeira operação real do modelo de teste.",
            )
            STATE["test_active"] = None
            return True, "WIN na operação REAL do teste. Modelo finalizado."

        # LOSS na operação real: tenta iniciar exatamente um Gale.
        if not can_start_new_operation(expiry):
            save_test_signal(
                test_active["signal_time"],
                test_active["real_entry_time"],
                test_active["direction"],
                "LOSS",
                used_gale=False,
                notes="LOSS na operação real; Gale não iniciado por limite de horário.",
            )
            STATE["test_active"] = None
            return True, "LOSS na operação REAL. Gale não iniciado por limite de horário."

        gale_entry = expiry
        test_active["stage"] = "GALE"
        test_active["start_time"] = iso_local(gale_entry)
        test_active["expiry_time"] = iso_local(gale_entry + timedelta(minutes=5))
        test_active["used_gale"] = True
        test_active["status"] = "EM_ANDAMENTO"
        STATE["test_active"] = test_active
        return True, "LOSS na operação REAL. Gale 1 iniciado."

    # Resultado do Gale do modelo de teste.
    if stage == "GALE":
        save_test_signal(
            test_active["signal_time"],
            test_active["real_entry_time"],
            test_active["direction"],
            result,
            used_gale=True,
            notes="Resultado final no Gale 1 do modelo de teste.",
        )
        STATE["test_active"] = None
        return True, f"Resultado final do teste: {result}."

    return False, "Etapa do modelo de teste desconhecida."


def process_test_active():
    """Controla os prazos do modelo de teste sem alterar o modelo original."""
    test_active = STATE.get("test_active")
    if not test_active:
        return

    now = now_local()
    expiry = parse_iso(test_active["expiry_time"])

    if now < expiry:
        return

    if test_active.get("status") == "EM_ANDAMENTO":
        test_active["status"] = "AGUARDANDO_RESULTADO"
        test_active["awaiting_result_since"] = iso_local(now)
        STATE["test_active"] = test_active


def process_active(candles):
    """
    Apura automaticamente o resultado pela vela de 5 minutos do BTC-USD.

    A mesma vela usada como referência no TradingView/Coinbase define o resultado:
      CALL + vela CALL = WIN
      PUT  + vela PUT  = WIN
      direção contrária = LOSS
      DOJI = aguarda uma próxima leitura válida

    Se a Entrada der LOSS, o G1 é iniciado automaticamente pela próxima vela.
    """
    active = STATE.get("active")
    if not active:
        return

    now = now_local()
    expiry = parse_iso(active["expiry_time"])

    if now < expiry:
        return

    # Só começa a apuração quando a vela da operação já terminou.
    try:
        start_time = parse_iso(
            active["entry_time"]
            if active["stage"] == "ENTRADA"
            else active["gale_entry_time"]
        )

        entry_price, exit_price, c = boundary_prices(candles, start_time, expiry)

        # A vela ainda não está disponível/consolidada.
        if c is None or entry_price is None or exit_price is None:
            return

        color = candle_color(c)

        # DOJI não define direção.
        if color not in ("CALL", "PUT"):
            return

        # Guarda a auditoria da mesma vela que define o resultado.
        ref = {
            "stage": active.get("stage"),
            "signal_direction": active.get("direction"),
            "candle_color": color,
            "candle_time": c.get("time"),
            "open": entry_price,
            "close": exit_price,
            "decision": "APURAÇÃO AUTOMÁTICA PELO TRADINGVIEW/COINBASE",
            "checked_at": iso_local(now_local()),
            "source": "TRADINGVIEW_CANDLE_AUTO",
        }

        STATE["last_result_audit"] = ref
        active["coinbase_reference"] = ref

        if active["stage"] == "ENTRADA":
            update_signal(
                active["id"],
                entry_price=entry_price,
                first_exit_price=exit_price,
            )
        else:
            update_signal(
                active["id"],
                gale_entry_price=entry_price,
                gale_exit_price=exit_price,
            )

        STATE["active"] = active

        # A própria cor da vela determina WIN/LOSS.
        result = "WIN" if color == active.get("direction") else "LOSS"

        # Libera a finalização automática após confirmar a vela encerrada.
        active["status"] = "AGUARDANDO_RESULTADO"
        STATE["active"] = active

        # Reaproveita a lógica existente de finalização/G1.
        submit_manual_result(result)

    except Exception:
        # Se a vela ainda não estiver pronta, tenta novamente no próximo ciclo.
        return

def finalize_test_model(signal_time, real_entry_time, direction, result, used_gale=False):
    """Registra somente o resultado FINAL do modelo de teste."""
    try:
        save_test_signal(
            signal_time=signal_time,
            real_entry_time=real_entry_time,
            direction=direction,
            result=result,
            used_gale=used_gale,
            notes="MODELO_TESTE_SEGUNDA_ENTRADA",
        )
    except Exception:
        pass


def test_first_operation_result(direction, candles, start_time):
    """
    Verifica a primeira operação do modelo de teste pela vela de 5 minutos.
    Esta etapa é apenas observação.
    """
    expiry = start_time + timedelta(minutes=5)
    entry_price, exit_price, candle = boundary_prices(candles, start_time, expiry)

    if candle is None:
        return None

    color = candle_color(candle)

    if color == direction:
        return "WIN"

    if color in ("CALL", "PUT"):
        return "LOSS"

    return None


def submit_manual_result(result):
    result = str(result or "").upper().strip()
    if result not in ("WIN", "LOSS"):
        return False, "Resultado inválido."

    active = STATE.get("active")
    if not active:
        return False, "Não existe operação aguardando resultado."
    if active.get("status") != "AGUARDANDO_RESULTADO":
        return False, "A operação ainda não chegou ao horário de expiração."

    now = now_local()
    expiry = parse_iso(active["expiry_time"])
    if now < expiry:
        return False, "A operação ainda está em andamento."

    if active["stage"] == "ENTRADA":
        update_signal(active["id"], first_manual_result=result, result_source="TRADINGVIEW_CANDLE_AUTO")

        if result == "WIN":
            update_signal(active["id"], result="WIN")

            # MODELO DE TESTE:
            # se a primeira oportunidade foi WIN, ela nunca vira
            # operação real. O teste é encerrado sem registrar operação.
            test_active = STATE.get("test_active")
            if test_active and test_active.get("stage") == "OBSERVACAO":
                STATE["test_active"] = None

            STATE["last_result_audit"] = {
                "stage": "ENTRADA",
                "signal_direction": active.get("direction"),
                "decision": "WIN apurado automaticamente pela vela BTC",
                "checked_at": iso_local(now),
                "source": "TRADINGVIEW_CANDLE_AUTO",
                **({k: v for k, v in (active.get("coinbase_reference") or {}).items() if k in ("candle_color", "open", "close")}),
            }
            STATE["active"] = None
            STATE["last_completed_ts"] = time.time()
            set_notice("WIN REGISTRADO", "Resultado apurado automaticamente pela vela BTC.", "win")
            return True, "WIN registrado."

        # LOSS na primeira: G1 faz parte do mesmo sinal e começa na vela imediatamente seguinte.
        # Regra de horário: depois das 16:00 não pode nascer um novo Gale.
        # Nesse caso a operação principal é encerrada como LOSS definitivo.
        # MODELO DE TESTE:
        # se a primeira oportunidade deu LOSS, a próxima vela vira
        # a primeira operação REAL do modelo de teste.
        test_active = STATE.get("test_active")
        if test_active and test_active.get("stage") == "OBSERVACAO":
            if can_start_new_operation(expiry):
                real_entry = expiry
                STATE["test_active"] = {
                    "signal_time": test_active["signal_time"],
                    "direction": test_active["direction"],
                    "stage": "REAL",
                    "start_time": iso_local(real_entry),
                    "expiry_time": iso_local(real_entry + timedelta(minutes=5)),
                    "real_entry_time": iso_local(real_entry),
                    "used_gale": False,
                    "status": "EM_ANDAMENTO",
                }
            else:
                STATE["test_active"] = None

        if not can_start_new_operation(expiry):
            update_signal(active["id"], result="LOSS")
            STATE["last_result_audit"] = {
                "stage": "ENTRADA",
                "signal_direction": active.get("direction"),
                "decision": "LOSS definitivo — janela 08:00-18:00 encerrada; G1 não iniciado",
                "checked_at": iso_local(now),
                "source": "TRADINGVIEW_CANDLE_AUTO",
            }
            STATE["active"] = None
            STATE["last_completed_ts"] = time.time()
            set_notice("LOSS REGISTRADO", "Janela operacional encerrada às 16:00. G1 não iniciado.", "loss")
            return True, "LOSS registrado. Janela encerrada; G1 não iniciado."

        # Como o app não enxerga o feed privado da corretora, quem confirma o LOSS é o usuário.
        # O horário lógico do G1 continua sendo a borda exata da vela, não o instante do clique.
        gale_start = expiry
        active["stage"] = "GALE"
        active["used_gale"] = True
        active["gale_entry_time"] = iso_local(gale_start)
        active["entry_time_original"] = active["entry_time"]
        active["expiry_time"] = iso_local(gale_start + timedelta(minutes=5))
        next_candle = candle_for_ts(STATE.get("candles") or [], gale_start)
        active["gale_entry_price"] = next_candle["open"] if next_candle else STATE.get("price")
        active["status"] = "GALE_EM_ANDAMENTO"
        active.pop("awaiting_result_since", None)
        active.pop("awaiting_stage", None)
        active.pop("coinbase_reference", None)
        update_signal(active["id"], used_gale=1, gale_entry_price=active.get("gale_entry_price"))
        STATE["active"] = active
        STATE["last_result_audit"] = {
            "stage": "ENTRADA",
            "signal_direction": active.get("direction"),
            "decision": "LOSS informado → G1",
            "checked_at": iso_local(now),
            "source": "TRADINGVIEW_CANDLE_AUTO",
        }
        STATE["notice"] = None
        return True, "LOSS registrado. G1 ativado."

    # Resultado do G1 encerra definitivamente o sinal.
    update_signal(
        active["id"],
        gale_manual_result=result,
        result=result,
        result_source="TRADINGVIEW_CANDLE_AUTO",
    )
    STATE["last_result_audit"] = {
        "stage": "GALE",
        "signal_direction": active.get("direction"),
        "decision": f"{result} do G1 apurado automaticamente pela vela BTC",
        "checked_at": iso_local(now),
        "source": "TRADINGVIEW_CANDLE_AUTO",
        **({k: v for k, v in (active.get("coinbase_reference") or {}).items() if k in ("candle_color", "open", "close")}),
    }
    STATE["active"] = None
    STATE["last_completed_ts"] = time.time()
    set_notice(
        f"{result} REGISTRADO",
        "Resultado do G1 apurado automaticamente pela vela BTC.",
        "win" if result == "WIN" else "loss",
    )
    return True, f"{result} do G1 registrado."

def market_data_refresh_loop():
    """Atualiza REST em segundo plano; falhas/lentidão não travam a decisão do sinal."""
    global REST_CANDLE_CACHE, REST_PRICE_CACHE, REST_LAST_OK, MARKET_DATA_ERROR
    while True:
        errors = []
        try:
            # O timeout existe apenas nesta thread; o monitor continua rodando.
            candles = fetch_candles()
            with market_data_lock:
                REST_CANDLE_CACHE = candles
        except Exception as e:
            errors.append(f"candles REST: {str(e)[:120]}")
        try:
            price = fetch_ticker()
            with market_data_lock:
                REST_PRICE_CACHE = price
        except Exception as e:
            errors.append(f"ticker REST: {str(e)[:120]}")
        with market_data_lock:
            if not errors:
                REST_LAST_OK = time.time()
                MARKET_DATA_ERROR = None
            else:
                MARKET_DATA_ERROR = " | ".join(errors)
        time.sleep(REST_REFRESH_SECONDS)


def get_cached_market_data():
    """Combina histórico REST com candles ao vivo sem esperar por chamadas de rede."""
    with market_data_lock:
        rest = list(REST_CANDLE_CACHE)
        price = REST_PRICE_CACHE
        last_ok = REST_LAST_OK
        err = MARKET_DATA_ERROR
    candles = merge_ws_candles(rest)
    # Quando o ticker REST falha, usa o fechamento do candle vivo mais recente.
    if price is None:
        current_bucket_ts = int(current_bucket_start(now_local()).astimezone(timezone.utc).timestamp())
        current = next((c for c in reversed(candles) if int(c["ts"]) == current_bucket_ts), None)
        if current:
            price = float(current["close"])
        elif candles:
            price = float(candles[-1]["close"])
    return candles, price, last_ok, err


def monitor_loop():
    while True:
        try:
            candles, price, last_ok, data_error = get_cached_market_data()
            if not candles or price is None:
                raise RuntimeError(data_error or "Aguardando os primeiros dados da Coinbase")
            analysis = analyze_market(candles, price)
            with state_lock:
                STATE["candles"] = candles
                STATE["price"] = price
                STATE["analysis"] = analysis
                STATE["last_update"] = iso_local(now_local())
                with ws_candles_lock:
                    ws_fresh = bool(WS_LAST_MESSAGE and time.time() - WS_LAST_MESSAGE <= 10)
                rest_fresh = bool(last_ok and time.time() - last_ok <= 20)
                STATE["feed_ok"] = ws_fresh or rest_fresh
                STATE["last_error"] = None if STATE["feed_ok"] else (data_error or "Dados sem atualização recente")
                clear_expired_notice()
                process_active(candles)
                process_test_active()
                process_pre_signal(candles)
                maybe_create_pre_signal(analysis)
        except Exception as e:
            with state_lock:
                STATE["feed_ok"] = False
                STATE["last_error"] = str(e)[:300]
                STATE["last_update"] = iso_local(now_local())
        time.sleep(POLL_SECONDS)


def safe_public_state():
    with state_lock:
        data = {
            "app": APP_NAME,
            "product": PRODUCT,
            "time": iso_local(now_local()),
            "price": STATE.get("price"),
            "analysis": STATE.get("analysis") or {},
            "pre_signal": STATE.get("pre_signal"),
            "active": STATE.get("active"),
            "notice": STATE.get("notice"),
            "feed_ok": STATE.get("feed_ok"),
            "last_error": STATE.get("last_error"),
            "last_update": STATE.get("last_update"),
            "last_result_audit": STATE.get("last_result_audit"),
            "session": session_status(),
            "stats": today_stats(),
            "recent": recent_results(20),
            "daily_financial": daily_financial_summary(90),
            "financial_settings": get_financial_settings(),
            "limits": {
                "signal_limit": DAILY_SIGNAL_LIMIT,
                "operations_today": count_operations_today(),
                "max_signals_hour": MAX_SIGNALS_PER_HOUR,
                "session_start": f"{SESSION_START_HOUR:02d}:00",
                "session_end": f"{SESSION_END_HOUR:02d}:00",
                "signals_this_hour": count_signals_this_hour(),
            },
            "candles": (STATE.get("candles") or [])[-36:],
        }
    return data


HTML = r'''<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#04070a">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<link rel="manifest" href="/manifest.json">
<meta name="theme-color" content="#04070a">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&display=swap" rel="stylesheet">
<title>BTC Signal Watch</title>
<style>
:root{--bg:#04070a;--panel:#080f14;--panel2:#0d171d;--text:#f2f7f9;--muted:#8fa3ad;--line:#16282e;--green:#12e58a;--green2:#0fb36b;--red:#ff3b4e;--amber:#f3b84b;--blue:#5ca9ff;--glow:0 0 18px rgba(18,229,138,.25)}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{background:var(--bg)}
body{margin:0;color:var(--text);font-family:'Rajdhani','Inter',system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;font-size:15px;background:radial-gradient(120% 60% at 50% -10%,rgba(18,229,138,.10),transparent 60%),var(--bg);min-height:100vh;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}
.wrap{max-width:560px;margin:auto;padding:14px 14px 28px}
b{font-weight:700}
/* topo */
.top{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:14px}
.brandRow{display:flex;align-items:center;gap:12px;min-width:0}
.logo{width:46px;height:46px;flex:none;border-radius:50%;background:radial-gradient(circle at 35% 30%,#ffb23a,#f7931a 60%,#d97a06);display:flex;align-items:center;justify-content:center;font-size:30px;font-weight:900;color:#fff;box-shadow:0 0 18px rgba(247,147,26,.45)}
.brand h1{font-size:20px;line-height:1.05;margin:0 0 4px;letter-spacing:.02em;font-weight:700}
.brand h1 em{font-style:normal;color:var(--green)}
.sub{color:var(--muted);font-size:13px;letter-spacing:.03em}
.pill{display:inline-flex;align-items:center;gap:6px;font-size:13px;color:var(--green);font-weight:600;max-width:104px;line-height:1.1;text-align:right;flex:none}
.dot{width:10px;height:10px;border-radius:50%;background:var(--amber);box-shadow:0 0 8px currentColor;color:var(--amber)}
.dot.ok{background:var(--green);color:var(--green)}
.pill:has(.dot:not(.ok)){color:var(--amber)}
.tools{display:flex;gap:8px;flex-wrap:wrap;margin:-4px 0 12px}
.notifyBtn{flex:1 1 auto;border:1px solid var(--line);border-radius:12px;padding:9px 12px;font:600 13px 'Rajdhani','Inter',system-ui,sans-serif;background:var(--panel);color:var(--text);cursor:pointer}
.notifyBtn.ok{border-color:#127a52;color:var(--green)}
.notifyBtn:disabled{opacity:.6}
/* cards */
.card{background:linear-gradient(180deg,rgba(13,23,29,.85),rgba(8,15,20,.95));border:1px solid var(--line);border-radius:18px;padding:15px;margin-top:12px}
.sectionTitle{display:flex;align-items:center;gap:10px;font-size:17px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px;color:#e9f3f6}
.sectionTitle i{font-style:normal;color:var(--green);font-size:20px}
.section{margin-top:12px}
.section h2{font-size:15px;margin:0 0 9px;letter-spacing:.05em;text-transform:uppercase}
/* hero / sinal atual */
.current{position:relative;min-height:128px;padding:16px 16px 16px 100px;border:1.5px solid var(--green2);background:linear-gradient(135deg,rgba(18,229,138,.10),rgba(8,15,20,.96) 55%);box-shadow:var(--glow),inset 0 0 30px rgba(18,229,138,.05);margin-top:0}
.current::before{content:"";position:absolute;left:18px;top:50%;width:62px;height:62px;margin-top:-31px;border-radius:50%;border:9px solid var(--green);box-shadow:0 0 16px rgba(18,229,138,.55),inset 0 0 12px rgba(18,229,138,.4);background:radial-gradient(circle,transparent 0 9px,rgba(18,229,138,.0) 10px)}
.current::after{content:"";position:absolute;left:39px;top:50%;width:20px;height:20px;margin-top:-10px;border-radius:50%;background:#04070a;border:5px solid var(--green)}
.current:has(.bigdir.put){border-color:#c22636;background:linear-gradient(135deg,rgba(255,59,78,.12),rgba(8,15,20,.96) 55%);box-shadow:0 0 18px rgba(255,59,78,.25)}
.current:has(.bigdir.put)::before{border-color:var(--red);box-shadow:0 0 16px rgba(255,59,78,.55),inset 0 0 12px rgba(255,59,78,.4)}
.current:has(.bigdir.put)::after{border-color:var(--red)}
.eyebrow{font-size:13px;letter-spacing:.12em;color:var(--muted);text-transform:uppercase;font-weight:600}
.bigdir{font-size:30px;line-height:1.05;font-weight:700;margin:6px 0 8px;letter-spacing:.02em}
.call{color:var(--green)}.put{color:var(--red)}.waiting{color:var(--green)}
.statusline{display:flex;flex-wrap:wrap;gap:7px;margin:9px 0}
.mini{border-radius:10px;background:rgba(18,229,138,.06);border:1px solid var(--line);padding:6px 9px;font-size:13px}
.reason{line-height:1.4;font-size:14px;color:#c4d3d9}
.alert{margin-top:10px;padding:9px 12px;border-radius:12px;background:#1f1a0d;border:1px solid #4a3a14;color:#f8d58d;font-size:13px;line-height:1.4}
.manualWarn{margin-top:10px;padding:9px 12px;border-radius:12px;background:#0d1a26;border:1px solid #234060;color:#b8d5ff;font-size:13px;line-height:1.4}
.progress{height:8px;background:#0f1b21;border-radius:99px;overflow:hidden;margin:10px 0}
.progress i{display:block;height:100%;background:linear-gradient(90deg,var(--green2),var(--green));width:0;box-shadow:0 0 10px rgba(18,229,138,.6)}
/* metricas */
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:12px}
.metric{padding:10px 8px;text-align:center;margin-top:0;border-radius:14px}
.metric .k{font-size:12px;color:var(--muted);letter-spacing:.08em;text-transform:uppercase;margin-bottom:3px}
.metric .v{font-size:18px;font-weight:700}
/* modelo de teste dentro de card */
#testModel{margin-top:0}
.testWrap{margin-top:12px}
/* grafico */
.chartHead{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap}
.chartHead .ttl{font-size:20px;font-weight:700;letter-spacing:.02em}
.tfs{display:flex;gap:2px;background:#0a1318;border:1px solid var(--line);border-radius:12px;padding:3px}
.tf{border:1px solid transparent;background:transparent;color:var(--muted);font:600 14px 'Rajdhani','Inter',sans-serif;padding:5px 10px;border-radius:9px;cursor:pointer}
.tf.on{color:var(--green);border-color:var(--green2);background:rgba(18,229,138,.12)}
.tvbox{width:100%;height:340px;background:var(--panel);border:1px solid var(--line);border-radius:14px;overflow:hidden}
.tvbox iframe{width:100%;height:100%;border:0}
.chart{width:100%;height:190px;background:#060c10;border:1px solid var(--line);border-radius:14px;margin-top:10px}
.chartNote{color:var(--muted);font-size:12px;margin-top:6px;letter-spacing:.03em}
.clockTag{color:var(--muted);font-size:13px}
/* calendario */
.calendar{display:block}
.calTop{display:flex;justify-content:space-between;align-items:center;gap:12px;margin:2px 0 12px;text-align:center;text-transform:uppercase;letter-spacing:.05em;font-size:16px;color:#cfe0e6}
.calNav{background:transparent;color:var(--text);border:0;font-size:26px;padding:0 10px;cursor:pointer;line-height:1}
.calGrid{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:6px}
.calWeek{text-align:center;color:var(--muted);font-size:13px;padding:4px 0;letter-spacing:.05em;text-transform:uppercase}
.calCell{min-width:0;min-height:42px;background:#0c161c;border:1px solid #14242b;border-radius:999px;color:#9fb1b9;padding:4px 2px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:0;cursor:pointer;font-family:inherit}
.calCell.empty{visibility:hidden}
.calCell.positive{background:linear-gradient(180deg,#0f6b43,#094a2f);border-color:#16a064;color:#eafff4;box-shadow:0 0 10px rgba(18,229,138,.18)}
.calCell.negative{background:linear-gradient(180deg,#8a1424,#5f0d19);border-color:#d3364a;color:#ffecee;box-shadow:0 0 10px rgba(255,59,78,.2)}
.calCell.zero{border-color:#77704a;color:#e8dfa6}
.calCell.selected{outline:2px solid var(--amber);outline-offset:1px}
.calNum{font-size:15px;font-weight:700;line-height:1.1}
.calValue{display:none}
.calCount{display:none}
.calDetail{margin-top:12px;padding:12px;background:#0a1318;border:1px solid var(--line);border-radius:12px;font-size:14px;line-height:1.7}
.legend{display:flex;gap:16px;flex-wrap:wrap;margin-top:12px;color:var(--muted);font-size:13px;letter-spacing:.04em;text-transform:uppercase}
.legend span{display:inline-flex;align-items:center;gap:6px}
.legend i{width:14px;height:14px;border-radius:50%;display:inline-block}
.legend .lw{background:var(--green);box-shadow:0 0 8px rgba(18,229,138,.6)}.legend .ll{background:var(--red);box-shadow:0 0 8px rgba(255,59,78,.6)}.legend .ln{background:#1a252b}
.dayCard{background:#0a1318;border:1px solid var(--line);border-radius:12px;padding:10px 12px;margin-bottom:8px}
.dayHead{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.dayResult.win,.dayResult.loss{font-size:16px}
.dayStats{font-size:13px;line-height:1.7;color:#c4d3d9}
/* desempenho */
.perf{display:grid;grid-template-columns:1fr 1px 1fr;gap:6px;align-items:stretch}
.perfCol .lbl{font-size:15px;color:var(--green);letter-spacing:.06em;text-transform:uppercase;margin-bottom:6px;font-weight:600}
.perfCol.tot .lbl{color:#cfe0e6}
.perfNums{display:flex;justify-content:space-around;gap:6px}
.perfNums div{text-align:center}
.perfNums b{display:block;font-size:34px;line-height:1;font-weight:700}
.perfNums small{font-size:13px;letter-spacing:.06em;text-transform:uppercase}
.perfNums .w{color:var(--green)}.perfNums .l{color:var(--red)}
.perfSep{background:var(--line)}
/* historico */
.history{display:grid;gap:8px}
.row{display:grid;grid-template-columns:64px 84px 1fr 64px;gap:8px;align-items:center;background:#0a1318;border:1px solid var(--line);border-radius:12px;padding:10px 12px;font-size:14px}
.tag{font-weight:700}
.win{color:var(--green);font-weight:700}.loss{color:var(--red);font-weight:700}.muted{color:var(--muted)}
.resultActions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}
.resultBtn{border:0;border-radius:12px;padding:12px 16px;font:700 16px 'Rajdhani','Inter',sans-serif;cursor:pointer;min-height:46px;flex:1}
.resultBtn.winBtn{background:#0b3a28;color:#72efbc;border:1px solid #138a5a}
.resultBtn.lossBtn{background:#43131b;color:#ff9aa3;border:1px solid #8a2634}
.resultBtn:disabled{opacity:.45;cursor:not-allowed}
.foot{color:#6f838c;font-size:11px;line-height:1.5;margin:18px 2px 5px}
.dock{display:flex;align-items:center;justify-content:center;gap:14px;margin-top:20px}
.dock .ln{flex:1;max-width:90px;height:3px;border-radius:3px;background:linear-gradient(90deg,transparent,var(--green));box-shadow:0 0 8px var(--green)}
.dock .ln.r{transform:scaleX(-1)}
.dock .bt{width:50px;height:50px;border-radius:50%;border:2px solid var(--green2);display:flex;align-items:center;justify-content:center;color:var(--green);font-size:26px;font-weight:900;box-shadow:0 0 16px rgba(18,229,138,.4);background:rgba(18,229,138,.06)}
label{display:block;font-size:13px;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;margin:0 0 5px}
input,select,textarea{width:100%;max-width:100%;padding:12px;border-radius:12px;border:1px solid #1d3a40;background:#060c10;color:var(--text);font:600 16px 'Rajdhani','Inter',system-ui,sans-serif;outline:none}
input:focus,select:focus,textarea:focus{border-color:var(--green);box-shadow:0 0 0 2px rgba(18,229,138,.15)}
#saveFinanceBtn{width:100%;border:1px solid var(--green2);border-radius:12px;padding:12px 16px;background:linear-gradient(180deg,#0f6b43,#094a2f);color:#eafff4;font:700 16px 'Rajdhani','Inter',sans-serif;letter-spacing:.05em;text-transform:uppercase;cursor:pointer;box-shadow:0 0 12px rgba(18,229,138,.2)}
#saveFinanceBtn:disabled{opacity:.5}
button{transition:filter .15s ease,transform .15s ease}button:not(:disabled):active{transform:scale(.98)}
@media(min-width:760px){.wrap{max-width:640px}}
@media(max-width:380px){.brand h1{font-size:19px}.logo{width:44px;height:44px;font-size:25px}.current{padding-left:88px}.current::before{left:14px;width:56px;height:56px;margin-top:-28px}.current::after{left:34px}.bigdir{font-size:26px}.calNum{font-size:13px}}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div class="brandRow">
      <div class="logo">₿</div>
      <div class="brand"><h1>BTC <em>SIGNAL</em> WATCH</h1><div class="sub">V1.8.6 &nbsp;|&nbsp; HORÁRIO BR 08-18</div></div>
    </div>
    <div class="pill"><span id="feedDot" class="dot"></span><span id="feedText">Conectando…</span></div>
  </div>

  <div class="tools"><button id="notifyBtn" class="notifyBtn" type="button" onclick="enableNotifications()">🔔 Ativar notificações</button><button id="testNotifyBtn" class="notifyBtn" type="button" onclick="testNotification()">🧪 Testar notificação</button></div>

  <div id="currentCard" class="card current"></div>

  <div class="testWrap">
    <div class="eyebrow" style="margin:0 0 6px 2px">🧪 Modelo de teste</div>
    <div id="testModel" class="card"></div>
  </div>

  <div class="grid">
    <div class="card metric"><div class="k">BTC/USD</div><div id="price" class="v">—</div></div>
    <div class="card metric"><div class="k">Sessão</div><div id="session" class="v" style="font-size:14px">—</div></div>
    <div class="card metric"><div class="k">Operações</div><div id="ops" class="v">0 / 4</div></div>
  </div>
  <div style="display:none"><span id="wins">0W</span><span id="losses">0L</span></div>

  <div class="card">
    <div class="chartHead">
      <div><div class="ttl">BTC/USD</div><div class="clockTag">TradingView • Coinbase • <span id="clock">—</span></div></div>
      <div class="tfs" id="tfs">
        <button class="tf" data-i="1" type="button">1m</button><button class="tf on" data-i="5" type="button">5m</button><button class="tf" data-i="15" type="button">15m</button><button class="tf" data-i="60" type="button">1h</button><button class="tf" data-i="240" type="button">4h</button>
      </div>
    </div>
    <div class="tvbox">
      <iframe id="tvFrame" src="https://www.tradingview.com/widgetembed/?frameElementId=tradingview_btc&symbol=COINBASE%3ABTCUSD&interval=5&hidesidetoolbar=0&symboledit=1&saveimage=0&toolbarbg=f1f3f6&studies=%5B%5D&theme=dark&style=1&timezone=America%2FSao_Paulo&withdateranges=1&hideideas=1" allowtransparency="true" frameborder="0" scrolling="no"></iframe>
    </div>
    <canvas id="chart" class="chart"></canvas>
    <div class="chartNote">Linha: fechamentos recentes • tracejado verde = suporte • tracejado vermelho = resistência</div>
  </div>

  <div class="card">
    <div class="sectionTitle"><i>▦</i> Calendário</div>
    <div id="calendarSummary" class="calendar"></div>
    <div class="legend"><span><i class="lw"></i>Win</span><span><i class="ll"></i>Loss</span><span><i class="ln"></i>Sem resultado</span></div>
    <h3 class="sectionTitle" style="margin-top:18px;font-size:15px"><i>🧪</i> Modelo de teste</h3>
    <div id="testCalendarSummary" class="calendar"></div>
  </div>

  <div class="card">
    <div class="sectionTitle"><i>$</i> Configuração financeira</div>
    <label for="entryStake">Entrada por operação (R$)</label>
    <input id="entryStake" type="number" min="0.01" max="1000000" step="0.01" value="6">
    <label for="payoutPercent" style="margin-top:12px">Payout (%)</label>
    <input id="payoutPercent" type="number" min="1" max="100" step="0.01" value="85">
    <div id="financePreview" class="muted" style="margin-top:12px;line-height:1.7;font-size:14px">Gale: R$ 12,00 · WIN direto: +R$ 5,10 · LOSS direto: −R$ 6,00</div>
    <button id="saveFinanceBtn" type="button" onclick="saveFinancialSettings()" style="margin-top:12px">Salvar financeiro</button>
    <div id="financeMessage" class="muted" style="margin-top:8px;font-size:14px"></div>
  </div>

  <div class="card">
    <div class="sectionTitle"><i>▥</i> Desempenho</div>
    <div class="perf">
      <div class="perfCol"><div class="lbl">Hoje</div><div class="perfNums"><div><b class="w" id="pfTodayW">0</b><small class="w">Wins</small></div><div><b class="l" id="pfTodayL">0</b><small class="l">Loss</small></div></div></div>
      <div class="perfSep"></div>
      <div class="perfCol tot"><div class="lbl">Total</div><div class="perfNums"><div><b class="w" id="pfTotalW">0</b><small class="w">Wins</small></div><div><b class="l" id="pfTotalL">0</b><small class="l">Loss</small></div></div></div>
    </div>
  </div>

  <div class="card">
    <div class="sectionTitle"><i>◎</i> Leitura atual</div>
    <div class="statusline"><span class="mini">Regime: <b id="trend">—</b></span><span class="mini">Setup: <b id="setup">—</b></span></div>
    <div class="reason">Score <b id="score">0</b>/100</div>
    <div class="progress"><i id="scorebar"></i></div>
    <div class="reason">Suporte recente: <b id="support">—</b><br>Resistência recente: <b id="resistance">—</b><br>RSI: <b id="rsi">—</b></div>
    <div id="reasons" class="alert">Analisando as velas recentes…</div>
  </div>

  <div class="card"><div class="sectionTitle"><i>✓</i> Auditoria da última apuração</div><div id="audit" class="reason muted">Nenhuma operação apurada nesta sessão.</div></div>
  <div class="section"><div class="sectionTitle" style="margin-top:6px"><i>≡</i> Sinais recentes</div><div id="history" class="history"></div></div>
  <div class="foot">Fonte de análise: Coinbase Exchange BTC-USD. Resultado oficial: informado manualmente por você conforme a sua corretora. A Coinbase exibida na auditoria é apenas referência e nunca decide WIN/LOSS. O contexto é travado 30s antes e a entrada ainda exige cor + rejeição forte no feed de análise. Notificações web: somente quando uma operação é liberada e confirmada; no Android usa Service Worker para exibição correta. O botão de teste não conta como operação. Ferramenta experimental de análise/paper-trading; não executa ordens nem garante resultado.</div>
  <div class="dock"><div class="ln"></div><div class="bt">₿</div><div class="ln r"></div></div>
</div>
<script>
const $=id=>document.getElementById(id);
const money=v=>v==null?'—':Number(v).toLocaleString('en-US',{style:'currency',currency:'USD',maximumFractionDigits:2});
const esc=s=>String(s??'').replace(/[&<>'"]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[m]));
function secLeft(iso){return Math.max(0,Math.floor((new Date(iso)-Date.now())/1000));}
function fmtSec(s){const m=Math.floor(s/60),x=s%60;return String(m).padStart(2,'0')+':'+String(x).padStart(2,'0');}

let lastNotificationKey=localStorage.getItem('btcSignalLastNotificationKey')||'';
let audioCtx=null;
let notificationRegistration=null;
async function prepareNotificationSystem(){
  try{
    if('serviceWorker' in navigator){
      notificationRegistration=await navigator.serviceWorker.register('/sw.js',{scope:'/'});
      await navigator.serviceWorker.ready;
    }
  }catch(e){ console.warn('Service Worker de notificações indisponível',e); }
}
async function testNotification(){
  if(typeof Notification==='undefined'){
    alert('Este navegador não oferece notificações nesta página.');
    return;
  }
  if(Notification.permission!=='granted'){
    alert('Primeiro toque em “Ativar notificações” e permita as notificações.');
    return;
  }
  try{ audioCtx=audioCtx || new (window.AudioContext||window.webkitAudioContext)(); await audioCtx.resume(); }catch(e){}
  const ok=await notifyUser('🧪 TESTE DE NOTIFICAÇÃO','Se você recebeu esta mensagem, as notificações estão funcionando. Este teste não conta como operação.','signal');
  if(!ok){ alert('O Android/Chrome não conseguiu exibir a notificação. Tente recarregar a página e testar novamente.'); }
}
function notificationReady(){
  return typeof Notification!=='undefined' && Notification.permission==='granted';
}
async function enableNotifications(){
  try{
    if(typeof Notification==='undefined'){
      alert('Este navegador não oferece notificações nesta página.');
      return;
    }
    const p=await Notification.requestPermission();
    if(p==='granted'){
      try{ audioCtx=new (window.AudioContext||window.webkitAudioContext)(); await audioCtx.resume(); }catch(e){}
      await prepareNotificationSystem();
      $('notifyBtn').textContent='🔔 Notificações ativas';
      $('notifyBtn').classList.add('ok');
      alert('Notificações ativadas. Agora toque em “Testar notificação”.');
    }else{
      $('notifyBtn').textContent='🔕 Notificações bloqueadas';
    }
  }catch(e){ console.warn('Falha ao ativar notificações',e); alert('Erro ao ativar notificações: '+e); }
}
function beep(kind='signal'){
  try{
    if(!audioCtx) return;
    const now=audioCtx.currentTime;
    const osc=audioCtx.createOscillator(), gain=audioCtx.createGain();
    osc.type='sine';
    osc.frequency.value=kind==='signal'?880:660;
    gain.gain.setValueAtTime(0.0001,now);
    gain.gain.exponentialRampToValueAtTime(0.16,now+0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001,now+0.35);
    osc.connect(gain);gain.connect(audioCtx.destination);
    osc.start(now);osc.stop(now+0.4);
  }catch(e){}
}
async function notifyUser(title,body,kind='signal'){
  beep(kind);
  if(!notificationReady()) return false;
  try{
    await prepareNotificationSystem();
    if(notificationRegistration && 'showNotification' in notificationRegistration){
      await notificationRegistration.showNotification(title,{body,tag:'btc-signal-watch',renotify:true,icon:'data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 width=%22192%22 height=%22192%22%3E%3Crect width=%22192%22 height=%22192%22 rx=%2232%22 fill=%22%23090b10%22/%3E%3Ctext x=%2296%22 y=%22120%22 text-anchor=%22middle%22 font-size=%22110%22%3E₿%3C/text%3E%3C/svg%3E'});
      return true;
    }
  }catch(e){ console.warn('Falha no Service Worker, tentando Notification API',e); }
  try{
    const n=new Notification(title,{body,tag:'btc-signal-watch',renotify:true});
    setTimeout(()=>{try{n.close()}catch(e){}},12000);
    return true;
  }catch(e){ console.warn('Notification API falhou',e); return false; }
}
function checkNotifications(d){
  // Notificação de preparação é enviada pelo backend uma única vez aos 10s.
  // Não repetir aviso quando a operação é liberada; a vela fechada decide.
}

function currentHtml(d){
  const a=d.active,p=d.pre_signal,an=d.analysis||{},n=d.notice;
  if(a){
    const dir=a.direction, cls=dir==='CALL'?'call':'put', left=secLeft(a.expiry_time);
    const waiting=a.status==='AGUARDANDO_RESULTADO';
    const stage=a.stage==='GALE'?'GALÉ':'ENTRADA';
    if(waiting){
      const first=a.stage==='ENTRADA';
      return `<div class="eyebrow">APURAÇÃO AUTOMÁTICA</div><div class="bigdir ${cls}">${dir} ${dir==='CALL'?'↑':'↓'}</div><div class="alert">${first?'A vela de 5 minutos terminou. O app está apurando automaticamente pelo fechamento do TradingView.':'A vela do G1 terminou. O resultado está sendo apurado automaticamente pelo fechamento do TradingView.'}</div><div class="manualWarn">Resultado automático pela vela BTC/USD de 5 minutos: abertura → fechamento. Não é necessário clicar em WIN ou LOSS.</div>`;
    }
    const label=a.stage==='GALE'?'GALÉ EM ANDAMENTO':'OPERAÇÃO AO VIVO';
    return `<div class="eyebrow">${label}</div><div class="bigdir ${cls}">${dir} ${dir==='CALL'?'↑':'↓'}</div><div class="statusline"><span class="mini">Entrada: <b>${new Date(a.stage==='GALE'?a.gale_entry_time:a.entry_time).toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'})}</b></span><span class="mini">Expira em: <b>${fmtSec(left)}</b></span><span class="mini">Score: <b>${a.score}</b></span></div><div class="reason">Referência Coinbase: <b>${money(a.stage==='GALE'?a.gale_entry_price:a.entry_price)}</b><br>Preço Coinbase atual: <b>${money(d.price)}</b><br>Setup: ${esc(a.setup)}${a.stage==='ENTRADA'?'<br><b>No fechamento, o app perguntará o resultado da SUA corretora.</b>':'<br><b>No fechamento do G1, informe WIN ou LOSS pela SUA corretora.</b>'}</div>`;
  }
  if(p){
    const dir=p.direction, cls=dir==='CALL'?'call':'put', left=secLeft(p.entry_time), lc=p.live_confirmation||{}, liveC=(d.analysis||{}).current_candle, liveColor=liveC?(Number(liveC.close)>Number(liveC.open)?'CALL':Number(liveC.close)<Number(liveC.open)?'PUT':'DOJI'):'—';
    const locked=!!p.context_locked;
    const risk=p.status==='EM_RISCO';
    const early=p.status==='AVISO_30S';
    const liveReady=!!lc.met;
    const title=early?(liveReady?'🔥 SINAL PRÉ-CONFIRMADO — PREPARE-SE':'⏱ JANELA DE 30s — AGUARDANDO CONFIRMAÇÃO'):(locked?'🟢 PREPARAR ENTRADA':risk?'⚠️ PRÉ-SINAL EM RISCO':'🟡 PRÉ-SINAL');
    const context=locked?'✅ TRAVADO':risk?'⚠️ EM RISCO':'✅ VÁLIDO';
    const colorIcon=lc.color_ok?'✅':'…';
    const rejectIcon=lc.rejection_ok?'✅':'…';
    let riskText=risk?`<div class="alert" style="margin-top:10px">${esc(p.risk_reason||'Contexto enfraquecendo.')} Se continuar assim, o sinal será cancelado ANTES da entrada.</div>`:'';
    let lockText=locked?`<div class="alert" style="margin-top:10px">${early?(liveReady?'Faltam até 30s. Cor + rejeição estão presentes AGORA: prepare a entrada, mas a confirmação oficial continua sendo o fechamento da vela.':'Faltam até 30s. Ainda não há cor + rejeição juntas; não considere a entrada confirmada.'):'Contexto travado com antecedência. Agora só falta a confirmação objetiva da vela; não haverá reanálise estrutural no segundo da entrada.'}</div>`:'';
    return `<div class="eyebrow">${title}</div><div class="bigdir ${cls}">${dir} ${dir==='CALL'?'↑':'↓'}</div><div class="statusline"><span class="mini">Entrada prevista: <b>${new Date(p.entry_time).toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'})}</b></span><span class="mini">Faltam: <b>${fmtSec(left)}</b></span><span class="mini">Contexto: <b>${context}</b></span></div><div class="alert">${esc(p.condition)}</div><div class="statusline"><span class="mini">Cor agora: <b>${esc(liveColor)}</b> ${liveColor==='CALL'?'🟢':liveColor==='PUT'?'🔴':'…'}</span><span class="mini">${esc(lc.rejection_label||'Rejeição forte')}: <b>${lc.rejection_ok?'SIM':'ainda não'}</b> ${rejectIcon}</span></div>${riskText}${lockText}`;
  }
  if(n){
    return `<div class="eyebrow">Aviso do último pré-sinal</div><div class="bigdir put">${esc(n.title)}</div><div class="alert">${esc(n.message)}</div><div class="reason" style="margin-top:10px">O aviso permanece visível por alguns instantes para você saber exatamente por que não houve entrada.</div>`;
  }
  let title='ANALISANDO MERCADO', msg='Nenhum sinal ativo. O sistema continua observando as velas recentes.';
  if(d.session==='LIMITE_ATINGIDO'){title='LIMITE DE TESTE ATINGIDO';msg='4 operações finalizadas hoje. Novos sinais pausados; análise continua ativa.'}
  else if(d.session==='OBSERVACAO'){title='MODO OBSERVAÇÃO';msg='Fora do horário 08:00–18:00. Analisa normalmente, mas não publica sinais.'}
  return `<div class="eyebrow">Status</div><div class="bigdir waiting">${title}</div><div class="reason">${msg}</div><div class="statusline"><span class="mini">Operações hoje: <b>${d.limits.operations_today}/${d.limits.signal_limit}</b></span><span class="mini">Sinais nesta hora: <b>${d.limits.signals_this_hour}/${d.limits.max_signals_hour}</b></span><span class="mini">Janela: <b>${an.window_candles||0} candles</b></span></div>`;
}
function drawChart(candles,an){
  const cv=$('chart'),ctx=cv.getContext('2d'),rect=cv.getBoundingClientRect(),dpr=window.devicePixelRatio||1;
  cv.width=Math.max(300,Math.floor(rect.width*dpr));cv.height=Math.floor(190*dpr);ctx.scale(dpr,dpr);
  const w=rect.width,h=190;ctx.clearRect(0,0,w,h); if(!candles||candles.length<2)return;
  const vals=candles.map(c=>c.close); if(an.support)vals.push(an.support);if(an.resistance)vals.push(an.resistance);
  let mn=Math.min(...vals),mx=Math.max(...vals);const pad=(mx-mn)*.12||1;mn-=pad;mx+=pad;
  const x=i=>12+i*(w-24)/(candles.length-1), y=v=>12+(mx-v)*(h-24)/(mx-mn);
  ctx.strokeStyle='#252c39';ctx.lineWidth=1;for(let i=1;i<4;i++){const yy=i*h/4;ctx.beginPath();ctx.moveTo(0,yy);ctx.lineTo(w,yy);ctx.stroke()}
  if(an.support){ctx.strokeStyle='#24d18f66';ctx.setLineDash([5,5]);ctx.beginPath();ctx.moveTo(0,y(an.support));ctx.lineTo(w,y(an.support));ctx.stroke()}
  if(an.resistance){ctx.strokeStyle='#ff5f6d66';ctx.beginPath();ctx.moveTo(0,y(an.resistance));ctx.lineTo(w,y(an.resistance));ctx.stroke();ctx.setLineDash([])}
  ctx.strokeStyle='#8cc7ff';ctx.lineWidth=2;ctx.beginPath();candles.forEach((c,i)=>{const xx=x(i),yy=y(c.close);i?ctx.lineTo(xx,yy):ctx.moveTo(xx,yy)});ctx.stroke();
  const last=candles[candles.length-1];ctx.fillStyle='#f4f7fb';ctx.beginPath();ctx.arc(x(candles.length-1),y(last.close),3,0,Math.PI*2);ctx.fill();
}
prepareNotificationSystem();


function brl(v){
  return Number(v || 0).toLocaleString('pt-BR',{
    style:'currency',
    currency:'BRL'
  });
}

let financeSettingsLoaded = false;

function updateFinancePreview(){
  const stake = Number($('entryStake').value);
  const payout = Number($('payoutPercent').value);
  const el = $('financePreview');
  if(!el) return;

  if(!Number.isFinite(stake) || stake <= 0 ||
     !Number.isFinite(payout) || payout < 1 || payout > 100){
    el.textContent = 'Informe uma entrada válida e payout entre 1% e 100%.';
    return;
  }

  el.textContent =
    `Gale: ${brl(stake*2)} · WIN direto: +${brl(stake*payout/100)} · ` +
    `LOSS direto: −${brl(stake)} · WIN no Gale: ${brl(-stake+stake*2*payout/100)} · ` +
    `LOSS no Gale: −${brl(stake*3)}`;
}

async function saveFinancialSettings(){
  const stake = Number($('entryStake').value);
  const payout = Number($('payoutPercent').value);
  const message = $('financeMessage');

  if(!Number.isFinite(stake) || stake < 0.01 || stake > 1000000){
    message.textContent = 'Entrada permitida: R$ 0,01 a R$ 1.000.000,00.';
    return;
  }
  if(!Number.isFinite(payout) || payout < 1 || payout > 100){
    message.textContent = 'O payout deve ficar entre 1% e 100%.';
    return;
  }

  const button = $('saveFinanceBtn');
  button.disabled = true;
  message.textContent = 'Salvando…';

  try{
    const r = await fetch('/api/financial-settings', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({entry_stake:stake, payout_percent:payout})
    });
    const j = await r.json();
    if(!r.ok || !j.ok) throw new Error(j.message || 'Não foi possível salvar.');
    financeSettingsLoaded = true;
    message.textContent = 'Configuração financeira salva.';
    updateFinancePreview();
    await tick();
  }catch(e){
    message.textContent = e.message || 'Erro ao salvar as configurações.';
  }finally{
    button.disabled = false;
  }
}

$('entryStake').addEventListener('input', updateFinancePreview);
$('payoutPercent').addEventListener('input', updateFinancePreview);
updateFinancePreview();

let calendarMonth = new Date();
let calendarSelectedDate = null;

function renderDailyFinancial(d){
  const days = d.daily_financial || [];
  const el = $('calendarSummary');
  if(!el) return;

  const byDate = {};
  days.forEach(day => { byDate[day.date] = day; });

  const year = calendarMonth.getFullYear();
  const month = calendarMonth.getMonth();
  const first = new Date(year, month, 1);
  const count = new Date(year, month + 1, 0).getDate();
  const offset = (first.getDay() + 6) % 7;
  const monthName = first.toLocaleDateString('pt-BR', {month:'long', year:'numeric'});

  let html = `<div class="calTop">
    <button class="calNav" onclick="changeCalendarMonth(-1)">‹</button>
    <b>${monthName.charAt(0).toUpperCase()+monthName.slice(1)}</b>
    <button class="calNav" onclick="changeCalendarMonth(1)">›</button>
  </div>
  <div class="calGrid">
    ${['Seg','Ter','Qua','Qui','Sex','Sáb','Dom'].map(x=>`<div class="calWeek">${x}</div>`).join('')}`;

  for(let i=0;i<offset;i++) html += '<div class="calCell empty"></div>';

  for(let n=1;n<=count;n++){
    const date = `${year}-${String(month+1).padStart(2,'0')}-${String(n).padStart(2,'0')}`;
    const day = byDate[date];
    const value = day ? Number(day.result) : 0;
    const cls = !day ? 'noTrade' : value > 0 ? 'positive' : value < 0 ? 'negative' : 'zero';
    const selected = calendarSelectedDate === date ? ' selected' : '';
    html += `<button class="calCell ${cls}${selected}" onclick="selectCalendarDay('${date}')">
      <span class="calNum">${n}</span>
      ${day ? `<span class="calValue">${value > 0 ? '+' : ''}${value.toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2})}</span><span class="calCount">${day.total} op.</span>` : '<span class="calValue">—</span>'}
    </button>`;
  }

  html += '</div>';

  const selected = calendarSelectedDate && calendarSelectedDate.startsWith(`${year}-${String(month+1).padStart(2,'0')}-`)
    ? calendarSelectedDate : null;
  if(selected && byDate[selected]){
    const day = byDate[selected];
    html += `<div class="calDetail">
      <b>${esc(day.date_br || selected)} · Resultado ${brl(day.result)}</b>
      <div>Operações: ${day.total} · 🟢 WIN: ${day.wins} · 🔴 LOSS: ${day.losses}</div>
      <div>WIN direto: ${day.direct_wins} · Gale WIN: ${day.gale_wins}</div>
      <div>LOSS direto: ${day.direct_losses} · Gale LOSS: ${day.gale_losses}</div>
      <div>Acumulado registrado: ${brl(day.accumulated)}</div>
    </div>`;
  } else {
    html += '<div class="calDetail muted">Toque em um dia para ver os detalhes. Dias sem operações aparecem com traço.</div>';
  }
  el.innerHTML = html;
}

function changeCalendarMonth(delta){
  calendarMonth = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth()+delta, 1);
  calendarSelectedDate = null;
  renderDailyFinancial(window.lastDashboardData || {});
}

function selectCalendarDay(date){
  calendarSelectedDate = date;
  renderDailyFinancial(window.lastDashboardData || {});
}


function renderTestModel(d){
  const el = $('testModel');
  if(!el) return;

  const t = d.test_active;

  if(!t){
    el.innerHTML = `
      <div class="eyebrow">Modelo de teste</div>
      <div class="bigdir waiting">AGUARDANDO SINAL</div>
      <div class="reason">A primeira oportunidade será apenas observada. Só haverá operação real se ela terminar em LOSS.</div>
    `;
    return;
  }

  const stage = t.stage || '';
  const status = t.status || '';
  let title = 'OBSERVANDO 1ª OPORTUNIDADE';
  let text = 'Essa primeira entrada é virtual. Se der WIN, o teste é cancelado. Se der LOSS, a próxima vela será a operação REAL.';

  if(stage === 'REAL'){
    title = 'OPERAÇÃO REAL DO TESTE';
    text = status === 'AGUARDANDO_RESULTADO'
      ? 'A operação real terminou. Marque o resultado abaixo.'
      : 'Operação real em andamento.';
  }else if(stage === 'GALE'){
    title = 'GALE 1 DO TESTE';
    text = status === 'AGUARDANDO_RESULTADO'
      ? 'O Gale terminou. Marque o resultado FINAL abaixo.'
      : 'Gale 1 em andamento.';
  }

  const buttons = status === 'AGUARDANDO_RESULTADO'
    ? `
      <div class="resultActions">
        <button class="resultBtn winBtn" onclick="sendTestResult('WIN')">🟢 WIN</button>
        <button class="resultBtn lossBtn" onclick="sendTestResult('LOSS')">🔴 LOSS</button>
      </div>
    `
    : '';

  el.innerHTML = `
    <div class="eyebrow">Modelo de teste • separado do modelo original</div>
    <div class="bigdir waiting">${title}</div>
    <div class="reason">${text}</div>
    <div class="statusline">
      <span class="mini">Direção: <b>${esc(t.direction || '—')}</b></span>
      <span class="mini">Início real: <b>${esc(t.real_entry_time || 'ainda não')}</b></span>
    </div>
    ${buttons}
  `;
}

function renderTestDailyFinancial(d){
  const el = $('testCalendarSummary');
  if(!el) return;

  const days = d.test_daily_financial || [];

  if(!days.length){
    el.innerHTML = '<div class="dayCard">Nenhum resultado do modelo de teste registrado ainda.</div>';
    return;
  }

  el.innerHTML = days.map(day => {
    const cls = Number(day.result) >= 0 ? 'win' : 'loss';

    return `
      <div class="dayCard">
        <div class="dayHead">
          <b>📅 ${esc(day.date_br || day.date)}</b>
          <span class="dayResult ${cls}">${brl(day.result)}</span>
        </div>
        <div class="dayStats">
          <div>Operações reais: <b>${day.total}</b></div>
          <div>🟢 WIN: <b>${day.wins}</b> &nbsp; 🔴 LOSS: <b>${day.losses}</b></div>
          <div>↪️ Gale WIN: <b>${day.gale_wins}</b></div>
          <div>↪️ Gale LOSS: <b>${day.gale_losses}</b></div>
          <div>📊 Acumulado: <b>${brl(day.accumulated)}</b></div>
        </div>
      </div>
    `;
  }).join('');
}

function render(d){
  window.lastDashboardData = d;
  if(!financeSettingsLoaded && d.financial_settings){
    $('entryStake').value = d.financial_settings.entry_stake ?? 6;
    $('payoutPercent').value = d.financial_settings.payout_percent ?? 85;
    financeSettingsLoaded = true;
    updateFinancePreview();
  }
  renderDailyFinancial(d);
  renderTestModel(d);
  renderTestDailyFinancial(d);
  $('feedDot').className='dot '+(d.feed_ok?'ok':''); $('feedText').textContent=d.feed_ok?'Feed conectado':'Feed com erro'; if(typeof Notification!=='undefined' && Notification.permission==='granted'){ $('notifyBtn').textContent='🔔 Notificações ativas'; $('notifyBtn').classList.add('ok'); }
  $('price').textContent=money(d.price); $('clock').textContent=new Date(d.time).toLocaleTimeString('pt-BR');
  $('ops').textContent=`${d.stats.total} / ${d.limits.signal_limit}`; $('wins').textContent=`${d.stats.wins}W`; $('losses').textContent=`${d.stats.losses}L`;
  $('session').textContent=d.session==='OPERACIONAL'?'SINAIS ATIVOS':d.session==='LIMITE_ATINGIDO'?'LIMITE BATIDO':'OBSERVAÇÃO';
  const an=d.analysis||{};$('trend').textContent=an.sideways?'LATERAL':(an.trend||'—');$('setup').textContent=an.setup||'—';$('score').textContent=an.score||0;$('scorebar').style.width=Math.min(100,an.score||0)+'%';
  $('support').textContent=money(an.support);$('resistance').textContent=money(an.resistance);$('rsi').textContent=an.rsi??'—';
  $('reasons').textContent=(an.reasons||['Analisando…']).join(' • ');$('currentCard').innerHTML=currentHtml(d);checkNotifications(d);
  const au=d.last_result_audit;
  $('audit').innerHTML=au?`Direção do sinal: <b>${esc(au.signal_direction)}</b> • Registro: <b>${esc(au.decision)}</b>${au.candle_color?`<br>Referência Coinbase: vela <b>${esc(au.candle_color)}</b> • OPEN ${money(au.open)} → CLOSE ${money(au.close)}`:''}<br><span class="muted">O resultado oficial é calculado automaticamente pela vela de 5 minutos do TradingView/Coinbase.</span>`:'Nenhuma operação apurada nesta sessão.';
  const hist=d.recent||[];$('history').innerHTML=hist.length?hist.map(r=>{
    const p1=(r.entry_price!=null&&r.first_exit_price!=null)?`${money(r.entry_price)} → ${money(r.first_exit_price)}`:'';
    const pg=(r.used_gale&&r.gale_entry_price!=null&&r.gale_exit_price!=null)?`<br><span class="muted">G1 ${money(r.gale_entry_price)} → ${money(r.gale_exit_price)}</span>`:'';
    return `<div class="row"><div><b>${esc(r.time)}</b><div class="muted">${esc(r.date)}</div></div><div class="tag ${r.direction==='CALL'?'call':'put'}">${esc(r.direction)} ${r.direction==='CALL'?'↑':'↓'}</div><div class="muted">${esc(r.setup||'')} ${r.used_gale?'• G1':''} • automático<br><span class="muted">${p1}</span>${pg}</div><div class="${r.result==='WIN'?'win':'loss'}">${esc(r.result)}</div></div>`
  }).join(''):'<div class="card muted">Ainda não há sinais finalizados.</div>';
  drawChart(d.candles,an);renderPerformance(d);
}
let sendingResult=false;
async function sendResult(result){
  if(sendingResult)return;
  sendingResult=true;
  document.querySelectorAll('.resultBtn').forEach(b=>b.disabled=true);
  try{
    const r=await fetch('/api/result',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({result})});
    const j=await r.json();
    if(!r.ok||!j.ok) alert(j.message||'Não foi possível registrar o resultado.');
    await tick();
  }catch(e){alert('Falha ao enviar o resultado para o app.');}
  finally{sendingResult=false;}
}
async function sendTestResult(result){
  if(sendingResult)return;
  sendingResult=true;
  document.querySelectorAll('#testModel .resultBtn').forEach(b=>b.disabled=true);
  try{
    const r=await fetch('/api/test_result',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({result})
    });
    const j=await r.json();
    if(!r.ok||!j.ok) alert(j.message||'Não foi possível registrar o resultado do teste.');
    await tick();
  }catch(e){
    alert('Falha ao enviar o resultado do modelo de teste.');
  }finally{
    sendingResult=false;
  }
}

async function tick(){try{const r=await fetch('/api/state',{cache:'no-store'});render(await r.json())}catch(e){$('feedText').textContent='Sem conexão com o servidor'}}

function renderPerformance(d){
  try{
    const st=d.stats||{};
    const set=(id,v)=>{const e=$(id);if(e)e.textContent=v;};
    set('pfTodayW',st.wins||0);set('pfTodayL',st.losses||0);
    let tw=0,tl=0;(d.daily_financial||[]).forEach(x=>{tw+=Number(x.wins||0);tl+=Number(x.losses||0);});
    set('pfTotalW',tw);set('pfTotalL',tl);
  }catch(e){}
}
document.querySelectorAll('#tfs .tf').forEach(b=>b.addEventListener('click',()=>{
  document.querySelectorAll('#tfs .tf').forEach(x=>x.classList.remove('on'));b.classList.add('on');
  const f=$('tvFrame');if(f)f.src=f.src.replace(/interval=[^&]*/,'interval='+b.dataset.i);
}));

tick();setInterval(tick,1000);window.addEventListener('resize',tick);
</script>
</body></html>'''


class Handler(BaseHTTPRequestHandler):
    server_version = "BTCSignalWatch/1.0"

    def log_message(self, fmt, *args):
        return

    def send_bytes(self, data, content_type="text/plain; charset=utf-8", status=200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            self.send_bytes(HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if path == "/manifest.json":
            manifest = {
                "id": "/",
                "name": "BTC Signal Watch",
                "short_name": "BTC Signal",
                "start_url": "/",
                "scope": "/",
                "display": "standalone",
                "display_override": ["standalone", "fullscreen"],
                "background_color": "#090b10",
                "theme_color": "#090b10",
                "icons": [
                    {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png"},
                    {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png"}
                ]
            }
            self.send_bytes(
                json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
                "application/manifest+json; charset=utf-8"
            )
            return
        if path == "/icon-192.png":
            self.send_bytes(Path("icon-192.png").read_bytes(), "image/png")
            return
        if path == "/icon-512.png":
            self.send_bytes(Path("icon-512.png").read_bytes(), "image/png")
            return
        if path == "/sw.js":
            sw = b"self.addEventListener('notificationclick', event => { event.notification.close(); });\\n"
            self.send_bytes(sw, 'application/javascript; charset=utf-8')
            return
        if path == "/api/state":
            payload = json.dumps(safe_public_state(), ensure_ascii=False).encode("utf-8")
            self.send_bytes(payload, "application/json; charset=utf-8")
            return
        if path == "/health":
            payload = json.dumps({"ok": True, "feed_ok": STATE.get("feed_ok"), "time": iso_local(now_local())}).encode("utf-8")
            self.send_bytes(payload, "application/json; charset=utf-8")
            return
        self.send_bytes(b"Not found", status=404)



# Acrescenta POST ao mesmo Handler sem depender de bibliotecas externas.
def _handler_do_POST(self):
    path = urlparse(self.path).path
    if path not in ("/api/result", "/api/financial-settings"):
        self.send_bytes(json.dumps({"ok": False, "message": "Not found"}).encode("utf-8"), "application/json; charset=utf-8", 404)
        return
    try:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0 or length > 4096:
            raise ValueError("Corpo inválido")
        body = json.loads(self.rfile.read(length).decode("utf-8"))

        if path == "/api/financial-settings":
            settings = save_financial_settings(
                body.get("entry_stake", 6),
                body.get("payout_percent", 85),
            )
            payload = json.dumps(
                {"ok": True, "message": "Configuração financeira salva.", "settings": settings},
                ensure_ascii=False,
            ).encode("utf-8")
            self.send_bytes(payload, "application/json; charset=utf-8", 200)
            return

        with state_lock:
            ok, message = submit_manual_result(body.get("result"))
        payload = json.dumps({"ok": ok, "message": message}, ensure_ascii=False).encode("utf-8")
        self.send_bytes(payload, "application/json; charset=utf-8", 200 if ok else 409)
    except Exception as e:
        payload = json.dumps({"ok": False, "message": f"Erro ao registrar: {str(e)[:160]}"}, ensure_ascii=False).encode("utf-8")
        self.send_bytes(payload, "application/json; charset=utf-8", 400)

Handler.do_POST = _handler_do_POST

def local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


if __name__ == "__main__":
    init_db()
    init_test_db()
    if websocket is not None:
        threading.Thread(target=websocket_candle_loop, daemon=True, name="coinbase-candles-ws").start()
    threading.Thread(target=market_data_refresh_loop, daemon=True, name="coinbase-rest-refresh").start()
    t = threading.Thread(target=monitor_loop, daemon=True, name="signal-monitor").start()
    ip = local_ip()
    print("\n" + "=" * 58)
    print(f" {APP_NAME}")
    print("=" * 58)
    print(f" No PC: http://127.0.0.1:{PORT}")
    print(f" Na mesma rede Wi-Fi: http://{ip}:{PORT}")
    print(" Horário dos sinais: 08:00–18:00 (UTC-3)")
    print(" VERSÃO ATIVA: V1.8.6 | REST em segundo plano + candles WebSocket")
    print(" Limites: 5 operações/dia | Máx.: 2 sinais/hora")
    print(" Pré-sinal: aviso único de preparação aos 10s; entrada depende da vela fechada")
    print(" Confirmação do sinal: cor exigida + rejeição forte")
    print(" Notificação: preparação aos 10s; confirmação final obrigatória | interface consulta a cada 1s")
    print(" Resultado: você marca WIN/LOSS conforme SUA corretora | Coinbase = referência")
    print(" Banco limpo desta versão: signals_pc_v1_8_manual.db")
    print(" Ctrl+C para encerrar.")
    print("=" * 58 + "\n")
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    try:
        threading.Timer(1.2, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")).start()
    except Exception:
        pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nEncerrando...")
    finally:
        server.server_close()
