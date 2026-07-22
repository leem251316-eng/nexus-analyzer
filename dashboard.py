#!/usr/bin/env python3
# =============================================================================
# NEXUS DASHBOARD  V1.0  (Jul 21 2026)
# Read-only mission control + living system map. Deploy as its own Railway
# service from the Trading-bot repo. Touches NO trading logic: it reads Alpaca
# (account/positions) and Postgres (completed trades, experiment progress) and
# serves a PIN-gated single-page app sized for phone + tablet.
#
# HARD GUARANTEE: this service has no write path to any broker or control
# endpoint. It cannot place, close, or modify a trade. Controls stay in
# Telegram by design. The PIN gates *viewing*, not spending.
#
# ENV REQUIRED (Railway):
#   DASHBOARD_PIN            e.g. 6-digit string, never in code
#   DATABASE_URL             shared Postgres (same as every service)
#   ALPACA_API_KEY / ALPACA_SECRET_KEY   read-only use here
#   DASHBOARD_SECRET         random string for session signing (any long value)
#   MISE_PYTHON_GITHUB_ATTESTATIONS=false   (all services need this)
# Start command:  python3 dashboard.py
# =============================================================================
import os
import time
import secrets
import threading
from functools import wraps
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from flask import Flask, request, jsonify, session, Response

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

try:
    from alpaca.trading.client import TradingClient
except ImportError:
    TradingClient = None

CENTRAL = ZoneInfo("America/Chicago")
VERSION = "V1.0"

PIN            = os.environ.get("DASHBOARD_PIN", "")
DATABASE_URL   = os.environ.get("DATABASE_URL", "")
API_KEY        = os.environ.get("ALPACA_API_KEY", "")
SECRET_KEY     = os.environ.get("ALPACA_SECRET_KEY", "")
LIVE_ERA_START = 1783314000   # Jul 6 2026 00:00 CDT — live fingerprint era

app = Flask(__name__)
app.secret_key = os.environ.get("DASHBOARD_SECRET", secrets.token_hex(32))

_trading = TradingClient(API_KEY, SECRET_KEY, paper=False) if (TradingClient and API_KEY) else None

# ---- tiny TTL cache so refreshing the app doesn't hammer Alpaca/PG ----------
_cache = {}
_cache_lock = threading.Lock()

def cached(key, ttl, producer):
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = producer()
    with _cache_lock:
        _cache[key] = (now, val)
    return val

def _db():
    if not psycopg2 or not DATABASE_URL:
        return None
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    return conn

# ---- auth -------------------------------------------------------------------
def require_pin(f):
    @wraps(f)
    def wrapper(*a, **k):
        if not session.get("ok"):
            return jsonify({"error": "unauthorized"}), 401
        return f(*a, **k)
    return wrapper

@app.route("/api/login", methods=["POST"])
def login():
    body = request.get_json(silent=True) or {}
    supplied = str(body.get("pin", ""))
    if PIN and secrets.compare_digest(supplied, PIN):
        session["ok"] = True
        session.permanent = True
        return jsonify({"ok": True})
    time.sleep(1.0)  # throttle brute force
    return jsonify({"ok": False}), 403

@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/health")
def health():
    return jsonify({"ok": True, "version": VERSION,
                    "db": bool(DATABASE_URL), "alpaca": bool(_trading)})

# ---- data producers ---------------------------------------------------------
def _alpaca_snapshot():
    if not _trading:
        return {"error": "alpaca-unavailable"}
    try:
        acct = _trading.get_account()
        positions = _trading.get_all_positions()
        pos = [{
            "symbol": p.symbol,
            "qty": float(p.qty),
            "market_value": round(float(p.market_value), 2),
            "unrealized_pl": round(float(p.unrealized_pl), 2),
            "unrealized_plpc": round(float(p.unrealized_plpc) * 100, 2),
            "current_price": round(float(p.current_price), 2),
        } for p in positions]
        return {
            "equity": round(float(acct.equity), 2),
            "cash": round(float(acct.cash), 2),
            "positions": pos,
            "position_count": len(pos),
        }
    except Exception as e:
        return {"error": str(e)[:120]}

def _today_bounds_ts():
    now = datetime.now(CENTRAL)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp()), int(now.timestamp())

def _live_fp_filter():
    # the decontamination rule, mirrored: live rows only
    return ("is_paper IS NOT TRUE AND trade_id NOT LIKE 'bt_%%' "
            "AND entry_ts >= %s AND exit_reason NOT IN ('trail','timeout')")

def _trades_today():
    conn = _db()
    if not conn:
        return {"error": "no-db"}
    start_ts, _ = _today_bounds_ts()
    out = {"trades": [], "wins": 0, "losses": 0}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT symbol, pnl_pct, exit_reason, won, exit_ts
                FROM berserker_trade_fingerprints
                WHERE {_live_fp_filter()} AND exit_ts >= %s
                ORDER BY exit_ts DESC
            """, (LIVE_ERA_START, start_ts))
            for r in cur.fetchall():
                out["trades"].append({
                    "symbol": r["symbol"],
                    "pnl_pct": round(r["pnl_pct"], 2) if r["pnl_pct"] is not None else None,
                    "exit_reason": r["exit_reason"],
                    "won": r["won"],
                    "t": datetime.fromtimestamp(r["exit_ts"], CENTRAL).strftime("%H:%M"),
                })
                if r["won"]:
                    out["wins"] += 1
                else:
                    out["losses"] += 1
    finally:
        conn.close()
    return out

def _v1053_gate():
    """
    V10.53 trailing experiment tracker. Pre-registered gate: 25 live exits
    post-deploy (Jul 15 18:26 CDT), WR >= 35%, avg loser <= -1.15%,
    gap-driven overnight losses excluded. Mirrors the manual bar so Matthew
    sees progress without a console.
    """
    conn = _db()
    if not conn:
        return {"error": "no-db"}
    deploy_ts = 1784158200  # Jul 15 2026 18:30 CDT (V10.55 boot, carries V10.53)
    res = {"target": 25, "wr_bar": 35, "loser_bar": -1.15}
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT won, pnl_pct, entry_ts, exit_ts, exit_reason
                FROM berserker_trade_fingerprints
                WHERE {_live_fp_filter()} AND exit_ts >= %s
            """, (LIVE_ERA_START, deploy_ts))
            rows = cur.fetchall()
    finally:
        conn.close()
    # gap loss = overnight (exit date > entry date) AND a stop
    def is_gap(entry_ts, exit_ts, reason):
        d_in = datetime.fromtimestamp(entry_ts, CENTRAL).date()
        d_out = datetime.fromtimestamp(exit_ts, CENTRAL).date()
        return d_out > d_in and reason and "stop" in reason
    counted = [r for r in rows if not is_gap(r[2], r[3], r[4])]
    n = len(counted)
    wins = sum(1 for r in counted if r[0])
    losers = [r[1] for r in counted if not r[0] and r[1] is not None]
    excluded = len(rows) - n
    res.update({
        "n": n,
        "wins": wins,
        "wr": round(wins / n * 100, 1) if n else 0.0,
        "avg_loser": round(sum(losers) / len(losers), 2) if losers else 0.0,
        "gap_excluded": excluded,
        "ready": n >= 25,
    })
    return res

def _gates_state():
    """Best-effort live gate lights from the most recent fingerprint + clocks."""
    now = datetime.now(CENTRAL)
    rth = (now.weekday() < 5) and (now.hour, now.minute) >= (8, 30) and now.hour < 15
    return {
        "rth": rth,
        "server_time": now.strftime("%a %b %d %H:%M CDT"),
        "crypto_locked": os.environ.get("CRYPTO_BUYS_DISABLED", "").lower() == "true",
    }

# ---- API endpoints ----------------------------------------------------------
@app.route("/api/overview")
@require_pin
def overview():
    alpaca = cached("alpaca", 15, _alpaca_snapshot)
    today  = cached("today", 20, _trades_today)
    gate   = cached("v1053", 60, _v1053_gate)
    gates  = _gates_state()
    return jsonify({"alpaca": alpaca, "today": today,
                    "experiment": gate, "gates": gates,
                    "version": VERSION, "ts": int(time.time())})

@app.route("/api/performance")
@require_pin
def performance():
    conn = _db()
    if not conn:
        return jsonify({"error": "no-db"}), 200
    cutoff = int(time.time()) - 14 * 86400
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT symbol,
                       COUNT(*) AS n,
                       SUM(CASE WHEN won THEN 1 ELSE 0 END) AS wins,
                       ROUND(AVG(pnl_pct)::numeric, 2) AS avg_pnl
                FROM berserker_trade_fingerprints
                WHERE {_live_fp_filter()} AND exit_ts >= %s
                GROUP BY symbol ORDER BY n DESC
            """, (LIVE_ERA_START, cutoff))
            rows = [{"symbol": r[0], "n": r[1], "wins": r[2],
                     "wr": round(r[2] / r[1] * 100) if r[1] else 0,
                     "avg_pnl": float(r[3]) if r[3] is not None else 0.0}
                    for r in cur.fetchall()]
    finally:
        conn.close()
    return jsonify({"symbols": rows, "window_days": 14})

# ---- frontend (single self-contained page) ----------------------------------
INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no"/>
<meta name="theme-color" content="#0a0e14"/>
<link rel="manifest" href="/manifest.json"/>
<title>NEXUS</title>
<style>
:root{
  --bg:#0a0e14; --panel:#141b26; --panel2:#1c2635; --line:#263345;
  --txt:#e6edf3; --dim:#7d8ca0; --grn:#3fb950; --red:#f85149;
  --amb:#d29922; --blu:#58a6ff; --acc:#bd93f9;
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{margin:0;background:var(--bg);color:var(--txt);
  font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:15px;
  padding-bottom:70px}
.hide{display:none!important}
header{position:sticky;top:0;z-index:20;background:rgba(10,14,20,.92);
  backdrop-filter:blur(8px);border-bottom:1px solid var(--line);
  padding:14px 16px;display:flex;align-items:center;justify-content:space-between}
header .brand{font-weight:700;letter-spacing:.14em;font-size:14px}
header .eq{font-variant-numeric:tabular-nums;font-weight:600}
.wrap{max-width:1000px;margin:0 auto;padding:16px}
.grid{display:grid;gap:12px;grid-template-columns:1fr}
@media(min-width:720px){.grid.two{grid-template-columns:1fr 1fr}
  .grid.three{grid-template-columns:1fr 1fr 1fr}}
.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;
  padding:16px}
.card h3{margin:0 0 10px;font-size:12px;letter-spacing:.1em;color:var(--dim);
  text-transform:uppercase;font-weight:600}
.big{font-size:30px;font-weight:700;font-variant-numeric:tabular-nums}
.row{display:flex;justify-content:space-between;align-items:center;
  padding:9px 0;border-bottom:1px solid var(--line)}
.row:last-child{border-bottom:0}
.sym{font-weight:600}.mono{font-variant-numeric:tabular-nums}
.pos{color:var(--grn)}.neg{color:var(--red)}.mut{color:var(--dim)}
.pill{display:inline-block;padding:3px 9px;border-radius:20px;font-size:12px;
  font-weight:600}
.pill.on{background:rgba(63,185,80,.15);color:var(--grn)}
.pill.off{background:rgba(248,81,73,.15);color:var(--red)}
.pill.warn{background:rgba(210,153,34,.15);color:var(--amb)}
.lights{display:flex;flex-wrap:wrap;gap:8px}
.light{display:flex;align-items:center;gap:6px;background:var(--panel2);
  border:1px solid var(--line);border-radius:10px;padding:8px 10px;font-size:13px}
.dot{width:9px;height:9px;border-radius:50%}
.dot.g{background:var(--grn)}.dot.r{background:var(--red)}.dot.a{background:var(--amb)}
.bar{height:9px;background:var(--panel2);border-radius:6px;overflow:hidden;
  margin-top:8px}
.bar>i{display:block;height:100%;background:linear-gradient(90deg,var(--blu),var(--acc))}
.nav{position:fixed;bottom:0;left:0;right:0;background:var(--panel);
  border-top:1px solid var(--line);display:flex;z-index:30}
.nav button{flex:1;background:none;border:0;color:var(--dim);padding:12px 0;
  font-size:11px;font-weight:600;letter-spacing:.05em}
.nav button.act{color:var(--blu)}
.nav button .ic{display:block;font-size:19px;margin-bottom:2px}
.center{min-height:70vh;display:flex;align-items:center;justify-content:center}
.pinbox{width:100%;max-width:320px;text-align:center}
.pinbox input{width:100%;font-size:26px;text-align:center;letter-spacing:.5em;
  padding:14px;border-radius:12px;border:1px solid var(--line);
  background:var(--panel);color:var(--txt);margin:16px 0}
.pinbox button{width:100%;padding:14px;border:0;border-radius:12px;
  background:var(--blu);color:#001;font-weight:700;font-size:16px}
.tiny{font-size:12px;color:var(--dim)}
.err{color:var(--red);font-size:13px;min-height:18px;margin-top:8px}
.evt{font-size:13px;padding:7px 0;border-bottom:1px solid var(--line);
  display:flex;gap:8px}
.evt .t{color:var(--dim);font-variant-numeric:tabular-nums;flex:0 0 44px}
/* system map */
svg{width:100%;height:auto;display:block}
.node{cursor:pointer}
.node rect{fill:var(--panel2);stroke:var(--line);stroke-width:1.5;rx:10}
.node.svc rect{stroke:var(--blu)}
.node.broker rect{stroke:var(--grn)}
.node.store rect{stroke:var(--acc)}
.node text{fill:var(--txt);font-size:12px;font-weight:600}
.node .sub{fill:var(--dim);font-size:9px}
.edge{stroke:var(--line);stroke-width:1.5;fill:none}
.sheet{position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:40;
  display:flex;align-items:flex-end;justify-content:center}
.sheet .inner{background:var(--panel);border:1px solid var(--line);
  border-radius:18px 18px 0 0;padding:20px;width:100%;max-width:600px;
  max-height:75vh;overflow:auto}
.sheet h2{margin:0 0 4px;font-size:18px}
.scn{background:var(--panel2);border:1px solid var(--line);border-radius:12px;
  padding:12px;margin-bottom:10px;cursor:pointer}
.scn b{color:var(--blu)}
.step{padding:8px 0 8px 26px;position:relative;border-bottom:1px solid var(--line);font-size:14px}
.step:before{content:attr(data-n);position:absolute;left:0;top:8px;
  width:18px;height:18px;border-radius:50%;background:var(--blu);color:#001;
  font-size:11px;font-weight:700;display:flex;align-items:center;justify-content:center}
</style></head>
<body>
<div id="app"></div>

<script>
const $=(s,r=document)=>r.querySelector(s);
const money=n=>(n<0?'-$':'$')+Math.abs(n).toFixed(2);
const pct=n=>(n>=0?'+':'')+n.toFixed(2)+'%';
const cls=n=>n>=0?'pos':'neg';
let state={tab:'home',data:null};

async function api(path,opts){
  const r=await fetch(path,Object.assign({headers:{'Content-Type':'application/json'}},opts));
  if(r.status===401){state.authed=false;render();throw new Error('unauth');}
  return r.json();
}

async function doLogin(){
  const pin=$('#pin').value.trim();
  $('#err').textContent='';
  const r=await api('/api/login',{method:'POST',body:JSON.stringify({pin})});
  if(r.ok){state.authed=true;load();}
  else{$('#err').textContent='Wrong PIN';}
}

async function load(){
  try{
    state.data=await api('/api/overview');
    state.authed=true;
    render();
  }catch(e){render();}
}

/* ---------- views ---------- */
function loginView(){
  return `<div class="center"><div class="pinbox">
    <div class="brand" style="letter-spacing:.2em;font-weight:700">🥩 NEXUS</div>
    <div class="tiny" style="margin-top:6px">Mission Control — read only</div>
    <input id="pin" type="tel" inputmode="numeric" placeholder="• • • • • •"
      autocomplete="off" onkeydown="if(event.key==='Enter')doLogin()"/>
    <button onclick="doLogin()">Unlock</button>
    <div class="err" id="err"></div>
    <div class="tiny" style="margin-top:14px">Controls stay in Telegram. This app can't move money.</div>
  </div></div>`;
}

function homeView(d){
  const a=d.alpaca||{},t=d.today||{},x=d.experiment||{},g=d.gates||{};
  const posRows=(a.positions||[]).map(p=>`<div class="row">
    <span><span class="sym">${p.symbol}</span> <span class="tiny mut">${p.qty} sh</span></span>
    <span class="mono ${cls(p.unrealized_plpc)}">${pct(p.unrealized_plpc)}
      <span class="tiny mut">${money(p.unrealized_pl)}</span></span></div>`).join('')
    || '<div class="tiny mut">No open positions</div>';
  const tRows=(t.trades||[]).slice(0,8).map(r=>`<div class="row">
    <span class="t tiny mut">${r.t}</span>
    <span class="sym">${r.symbol}</span>
    <span class="mono ${r.won?'pos':'neg'}">${r.pnl_pct==null?'':pct(r.pnl_pct)}</span>
    <span class="tiny mut">${r.exit_reason||''}</span></div>`).join('')
    || '<div class="tiny mut">No closed trades today</div>';
  const wl=(t.wins||0)+(t.losses||0);
  const wr=wl?Math.round((t.wins||0)/wl*100):0;
  const gapNote=x.gap_excluded?` · ${x.gap_excluded} gap excl`:'';
  const prog=Math.min(100,Math.round((x.n||0)/(x.target||25)*100));
  return `
  <div class="grid two">
    <div class="card"><h3>Berserker · Alpaca</h3>
      <div class="big">${a.equity!=null?money(a.equity):'—'}</div>
      <div class="tiny mut">cash ${a.cash!=null?money(a.cash):'—'} · ${a.position_count||0} open</div>
    </div>
    <div class="card"><h3>Today</h3>
      <div class="big">${t.wins||0}<span class="mut" style="font-size:20px">W</span>
        ${t.losses||0}<span class="mut" style="font-size:20px">L</span></div>
      <div class="tiny mut">${wr}% win rate · ${wl} trades</div>
    </div>
  </div>

  <div class="card"><h3>Open positions</h3>${posRows}</div>

  <div class="card"><h3>System gates</h3><div class="lights">
    <div class="light"><span class="dot ${g.rth?'g':'r'}"></span>RTH ${g.rth?'open':'closed'}</div>
    <div class="light"><span class="dot ${g.crypto_locked?'r':'g'}"></span>Crypto ${g.crypto_locked?'LOCKED':'open'}</div>
    <div class="light"><span class="dot g"></span>${g.server_time||''}</div>
  </div></div>

  <div class="card"><h3>V10.53 trailing gate</h3>
    <div class="row"><span>Exits counted</span>
      <span class="mono">${x.n||0} / ${x.target||25}${gapNote}</span></div>
    <div class="bar"><i style="width:${prog}%"></i></div>
    <div class="row" style="margin-top:8px"><span>Win rate</span>
      <span class="mono ${(x.wr||0)>=(x.wr_bar||35)?'pos':'neg'}">${(x.wr||0).toFixed(1)}%
        <span class="tiny mut">bar ${x.wr_bar||35}%</span></span></div>
    <div class="row"><span>Avg loser</span>
      <span class="mono ${(x.avg_loser||0)>=(x.loser_bar||-1.15)?'pos':'neg'}">${pct(x.avg_loser||0)}
        <span class="tiny mut">bar ${pct(x.loser_bar||-1.15)}</span></span></div>
    <div class="tiny mut" style="margin-top:6px">${x.ready?'Gate reached — ready for verdict':'Accumulating exits'}</div>
  </div>

  <div class="card"><h3>Closed today</h3>${tRows}</div>`;
}

/* ---------- system map ---------- */
const NODES={
  alpaca:{x:20,y:20,w:120,h:44,t:'Alpaca',s:'equities broker',c:'broker'},
  coinbase:{x:20,y:90,w:120,h:44,t:'Coinbase',s:'crypto · LOCKED',c:'broker'},
  bers:{x:200,y:20,w:130,h:44,t:'Berserker',s:'main.py V10.55',c:'svc'},
  scan:{x:200,y:90,w:130,h:44,t:'Scanner',s:'scanner.py V2.18',c:'svc'},
  crypto:{x:200,y:160,w:130,h:44,t:'Crypto',s:'crypto.py V5.22',c:'svc'},
  phase4:{x:200,y:230,w:130,h:44,t:'Phase4',s:'V2.16 · 4 bots',c:'svc'},
  db:{x:390,y:110,w:120,h:54,t:'Postgres',s:'shared state',c:'store'},
  tbone:{x:390,y:230,w:120,h:44,t:'T-Bone',s:'Telegram',c:'svc'},
};
const EDGES=[['alpaca','bers'],['alpaca','scan'],['alpaca','phase4'],
  ['coinbase','crypto'],['bers','db'],['scan','db'],['crypto','db'],
  ['phase4','db'],['db','tbone'],['bers','tbone']];
const NODE_INFO={
  alpaca:'Equities broker. Read-only keys feed this dashboard; Berserker, Scanner, and Phase4 trade through it.',
  coinbase:'Crypto broker. Live entries are hard-locked (CRYPTO_BUYS_DISABLED=true). Only the research/paper engine and exits run.',
  bers:'Momentum equities + Fleet Commander. Owns every T-Bone command. Stops -1.1%, TP ~1.5-2%, trailing arms only after +1.5% peak (V10.53).',
  scan:'Breakout equities on a 44-symbol universe. Runs in-process with Berserker. Scanner Thorn shadow-observes blocked setups.',
  crypto:'Mean-reversion on Coinbase, frozen for research. Paper engine has a 30-min loss cooldown (V5.22). Thorn captures the full tape.',
  phase4:'Four leveraged-ETF bots (bull+bear legs). Own Flask server, own state machine. ~86% of volume is TQQQ.',
  db:'One Postgres shared by every service: fingerprints, pattern memory, WinFollower stats, Thorn tape. The nervous system.',
  tbone:'Telegram bot. The mouth (alerts via send_alert) and the hands (every control command). This dashboard is the eyes only.',
};
const SCENARIOS={
  cb:{title:'3 stops → circuit breaker',steps:[
    'Berserker takes a 3rd consecutive stop-loss (_consecutive_losses hits CONSEC_LOSS_LIMIT=3).',
    'trigger_circuit_breaker() sets _circuit_break_until = now + 2h. New entries blocked.',
    'T-Bone posts the CB alert: "Send /resume to override early."',
    'BRANCH A — you send /resume: V10.55 zeroes _circuit_break_until + _consecutive_losses; entries resume next sweep.',
    'BRANCH B — you do nothing: entries resume automatically when the 2h timer expires.',
    'At 8:00 AM the daily reset also zeroes _consecutive_losses so an overnight gap stop cannot pre-load the counter.'],},
  earn:{title:'Earnings blackout',steps:[
    'The earnings feed flags a symbol reporting within the blackout window (e.g. TSLA).',
    'That symbol is added to the entry gate: no NEW positions opened in it.',
    'An already-open position is NOT force-closed — normal TP/SL/EOD still manage it.',
    'Practical: flatten it before the report yourself, or let EOD auto-close carry only if trending.',
    'After the report clears, the block lifts on the next feed refresh (every 4h).'],},
  crypto:{title:'Why crypto is locked',steps:[
    'Research found only ~1-2% of 4h windows clear Coinbase\'s 2.4% round-trip fee bar.',
    'CRYPTO_BUYS_DISABLED=true is a HARD env override (V5.22): /control cannot re-enable it.',
    'Fleet Commander\'s sync loop pushes buys_disabled=False every ~5 min — the hard flag rejects it.',
    'Exits, recovery, paper engine, and Thorn capture all keep running. Only live entries are frozen.',
    'Unlock is deliberate: flip the Railway var + redeploy, gated on the revival plan\'s phase criteria.'],},
  cooldown:{title:'Red crypto day → paper cooldown',steps:[
    'On falling tape the ungated paper engine used to re-enter a pair ~27s after each losing exit.',
    'That produced hundreds of near-identical losers, poisoning PatternMemory (which live gates read).',
    'V5.22: after a LOSING paper exit, that pair is blocked for 30 min (PAPER_LOSS_COOLDOWN_SECS=1800).',
    'Winners are exempt — they can re-enter immediately.',
    'paper_cooldown_check.py verifies: pre-fix median gap ~27s vs post-fix >=1800s.'],},
};

function mapView(){
  const edges=EDGES.map(([a,b])=>{
    const A=NODES[a],B=NODES[b];
    const x1=A.x+A.w,y1=A.y+A.h/2,x2=B.x,y2=B.y+B.h/2;
    const mx=(x1+x2)/2;
    return `<path class="edge" d="M${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}"/>`;
  }).join('');
  const nodes=Object.entries(NODES).map(([k,n])=>`
    <g class="node ${n.c}" onclick="nodeTap('${k}')">
      <rect x="${n.x}" y="${n.y}" width="${n.w}" height="${n.h}" rx="10"></rect>
      <text x="${n.x+12}" y="${n.y+22}">${n.t}</text>
      <text class="sub" x="${n.x+12}" y="${n.y+36}">${n.s}</text>
    </g>`).join('');
  const scn=Object.entries(SCENARIOS).map(([k,s])=>`
    <div class="scn" onclick="scnTap('${k}')"><b>▶</b> ${s.title}</div>`).join('');
  return `
  <div class="card"><h3>How NEXUS connects</h3>
    <svg viewBox="0 0 530 290">${edges}${nodes}</svg>
    <div class="tiny mut" style="margin-top:8px">Tap any box for detail.</div>
  </div>
  <div class="card"><h3>Scenario playbooks</h3>
    <div class="tiny mut" style="margin-bottom:10px">If this happens → what fires, in order.</div>
    ${scn}
  </div>`;
}

function nodeTap(k){
  openSheet(NODES[k].t, `<div class="tiny mut" style="margin-bottom:8px">${NODES[k].s}</div>
    <div>${NODE_INFO[k]||''}</div>`);
}
function scnTap(k){
  const s=SCENARIOS[k];
  const steps=s.steps.map((t,i)=>`<div class="step" data-n="${i+1}">${t}</div>`).join('');
  openSheet(s.title,steps);
}
function openSheet(title,html){
  const el=document.createElement('div');
  el.className='sheet';el.onclick=e=>{if(e.target===el)el.remove()};
  el.innerHTML=`<div class="inner"><h2>${title}</h2>${html}
    <div style="text-align:center;margin-top:14px">
      <button class="tiny" style="background:none;border:0;color:var(--blu);font-size:14px"
      onclick="this.closest('.sheet').remove()">Close</button></div></div>`;
  document.body.appendChild(el);
}

/* ---------- shell ---------- */
function render(){
  const app=$('#app');
  if(!state.authed){app.innerHTML=loginView();return;}
  const d=state.data||{};
  let body='';
  if(state.tab==='home')body=homeView(d);
  else if(state.tab==='map')body=mapView();
  const eq=(d.alpaca&&d.alpaca.equity!=null)?money(d.alpaca.equity):'';
  app.innerHTML=`
    <header><span class="brand">🥩 NEXUS</span>
      <span class="eq">${eq}</span></header>
    <div class="wrap"><div class="grid">${body}</div></div>
    <div class="nav">
      <button class="${state.tab==='home'?'act':''}" onclick="go('home')">
        <span class="ic">📊</span>Dashboard</button>
      <button class="${state.tab==='map'?'act':''}" onclick="go('map')">
        <span class="ic">🕸️</span>System</button>
      <button onclick="load()"><span class="ic">🔄</span>Refresh</button>
    </div>`;
}
function go(t){state.tab=t;render();}

/* boot: try session, else login */
load();
setInterval(()=>{if(state.authed&&state.tab==='home')load();},30000);
</script>
</body></html>"""

MANIFEST = """{
  "name":"NEXUS Mission Control","short_name":"NEXUS",
  "start_url":"/","display":"standalone",
  "background_color":"#0a0e14","theme_color":"#0a0e14",
  "icons":[{"src":"/icon.png","sizes":"512x512","type":"image/png"}]
}"""

@app.route("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")

@app.route("/manifest.json")
def manifest():
    return Response(MANIFEST, mimetype="application/json")

@app.route("/icon.png")
def icon():
    # 1x1 transparent PNG fallback so install doesn't 404; replace later if wanted
    import base64
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")
    return Response(png, mimetype="image/png")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    if not PIN:
        print("[dashboard] WARNING: DASHBOARD_PIN not set — login will reject all.")
    print(f"[dashboard] NEXUS Dashboard {VERSION} starting on :{port} "
          f"| db={bool(DATABASE_URL)} alpaca={bool(_trading)}")
    app.run(host="0.0.0.0", port=port, threaded=True)
