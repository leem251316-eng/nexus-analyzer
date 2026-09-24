Authored Sep 24 2026 for Fuzzy↔Matthew; observation-only; BAND_0905 owns live WR slot.

# Nexus Crypto Book — Deep Dive for Fuzzy ↔ Matthew

**As of:** Thu Sep 24, 2026 CT  
**Constraint:** BAND_0905 is the open WR-touching live experiment through Oct 15. This note is observation / paper / analyzer only. **Do not propose flipping `CRYPTO_BUYS_DISABLED`.**

**Repos:** Under the current architecture (Coinbase spot IOC taker, ~2.4% round-trip fee hurdle, RSI-gated multi-pair mean-reversion/momentum with TP ≈ fee), fee-cleared positive expectancy is not shown. Sunday suite re-prints ~−2.2 to −2.6%/trade net at 15–23% WR. Reopening live needs a different strategy class and/or venue fee regime — not a parameter tweak on V5.x.

---

## 1. What we run today (mechanism, not vibes)

### Deployment topology
| Piece | Where | Source |
| --- | --- | --- |
| Live crypto engine | Railway `rare-perception` → service **nexus-crypto** | `leem251316-eng/Trading-bot` `main`, start: `python -c "import crypto; crypto.run()"` |
| Sunday suite | Railway `rare-perception` → **nexus-backtester** | `leem251316-eng/nexus-analyzer`, cron `0 23 * * 0` UTC, `python run_all_backtests.py` |
| Research / preregs | Public `nexus-analyzer` | Analyzer-writable; Trading-bot writes are Matthew-only |

Latest nexus-crypto deploy observed: SUCCESS ~Sep 24, 2026 15:03 CT. Env var **`CRYPTO_BUYS_DISABLED` is defined** on the service (value not readable via OAuth MCP — confirm in Railway UI). Related: `CB_*` keys, `CB_TAKER_FEE_PCT`, Telegram, `DATABASE_URL`. No Alpaca live trading keys on nexus-crypto (only options paper keys listed).

**Venue split (easy to miss):**  
- **Live orders = Coinbase Advanced** via `CoinbaseClient` (`place_market_buy` → `market_market_ioc` quote_size).  
- **Sunday BT data = Alpaca crypto bars** (`BTC/USD` etc.) as a proxy — explicitly called out as imperfect in `crypto.py` V5.10 PatternMemory hygiene.  
- **Berserker / ~$295 Alpaca float** is the equities book. `capital_coordinator.py` coordinates Berserker↔Phase4 on Alpaca only; **crypto is not in that coordinator**. Crypto capital is Coinbase USDC balance.

### Code identity
- File: `Trading-bot/crypto.py` (~282 KB). Header version string: **V5.24** (Aug 13 2026 D1 shadow). Lock machinery landed V5.20/V5.21; user shorthand “V5.22 LOCKED” still describes the binding state.
- Companion: `control_server_crypto.py` (Flask `/snapshot|/trades|/status|/control|/health`; `/control` accepts `buys_disabled`).
- Analyzer twin: `nexus-analyzer/crypto_backtester.py` **V3.1** (fee-truth Jul 8) — this is what Sunday runs, not the older Trading-bot `crypto_backtester.py` V3.0 (fee-blind relative to V3.1).

### Universe
`PAIRS` (10): BTC, ETH, SOL, DOGE, XRP, DOT, ADA, LTC, POL, SUI — all `*-USDC`. Correlation groups cap 1 open per group. AVAX/LINK retired.

### Entry (live)
1. Multi-TF RSI / OBV / trend / VWAP / F&G / session / orderflow (Binance L/S, taker, OI; Coinbase L2) → confidence 0–100.  
2. Hard RSI gate from `RECIPES[*].rsi_entry_max` (BTC/ETH 38, XRP 30, …). Live logs Sep 24 CT: every scan cycle **BLOCKED(RSI 5m above gate)** — engine scanning, not buying.  
3. Modes: FULL ≥75, CAUTIOUS ≥55, SKIP ≥40, else BLOCK.  
4. Hour gates (CDT): PEAK / CAUTIOUS / NO_BUY + per-pair `avoid_hours`; Friday block; BTC vol regime blocks alts when 7d realized vol > 5%.  
5. Win Follower tiers (HOT/WARM/COLD/bench) adjust gating/sizing only.  
6. Gate: `try_enter` returns False if `paused` or **`buys_disabled`**.

### Exit
Stop / fee-aware TP (`tp_pct + ROUND_TRIP_FEE_PCT`) / partial at 50% of gross TP / ATR trail / trend-break / session+funding exits (fee-aware since V5.17) / time failsafe (~4h).

### Sizing (`_calc_trade_size`)
`base = USDC_balance / free_slots * 0.85`, × confidence mult × streak mult × HOT 1.30, floor $5, **cap 40% of balance**. Max positions: 3 weekday / 2 weekend.

### Fees / slip assumptions
| Path | Assumption |
| --- | --- |
| Live | Real Coinbase fill fees when available; fallback `CB_TAKER_FEE_PCT` (default **1.2%/side** → **2.4% RT**). Market IOC = always taker. |
| Paper | Same fee model; `$80` virtual size; `PAPER_MIN_CONFIDENCE=10`; **not gated by `CRYPTO_BUYS_DISABLED`** (V5.20: exits, recovery, paper, Thorn continue). |
| Sunday BT V3.1 | Same `CB_TAKER_FEE_PCT` + 0.05% half-spread slip; net PnL / won labels. |

### Lock behavior (binding)
- Boot: `buys_env_locked = CRYPTO_BUYS_DISABLED ∈ {1,true,yes}` → `buys_disabled=True`.  
- V5.21: `/control` may set True; **False is rejected** while env lock is on (Fleet Commander sync used to clobber — Jul 11 leak 0W/4L −$4.45). Lift = flip Railway var + redeploy.  
- V5.24: D1 shadow writes `crypto_shadow_signals` + Telegram; **zero order paths**; lock untouched (`CRYPTO_P2_SHADOW_PREREG.md`).

### Paper vs live paths
- Live: `run_buy_loop` (20s) → `try_enter` → Coinbase IOC.  
- Paper: `CryptoPaperEngine` separate threads; deliberately ungated for fingerprint seeding (historical contamination of CONF auto-tuner fixed in V5.5 — LIVE-only for threshold nudges).

---

## 2. Why the lock is binding (numbers + defects)

### Sunday suite (fee-aware V3.1) — Railway nexus-backtester

**Sep 19, 2026 ~17:37 CT** (and near-identical reprint completing ~Sep 20/21 overnight CT):

| Pair | Trades | WR | Avg PnL (net) |
| --- | --- | --- | --- |
| XRP-USDC | 83 | 22.9% | −2.410% |
| SOL-USDC | 89 | 21.3% | −2.222% |
| ETH-USDC | 92 | 20.7% | −2.173% / −2.189% |
| DOT-USDC | 94 | 19.1% | −2.492% |
| ADA-USDC | 68–69 | 19.1–20.3% | −2.524% / −2.537% |
| BTC-USDC | 82 | 17.1% | −2.316% / −2.317% |
| LTC-USDC | 97 | 16.5% | −2.551% / −2.556% |
| DOGE-USDC | 92 | 15.2% | −2.595% |
| POL/SUI | — | — | EMPTY bars (Alpaca map gap) |

**Aggregate:** ~697–698 trades, **~18.9–19.1% WR**.  
**By strategy:** MEAN_REVERSION ~167t / 19.8% WR / **avg −2.489%**; MOMENTUM ~530–531t / ~18.7–18.8% WR / **avg −2.387%**.  

Matches binding context (−2.2 to −2.6%/trade, 15–23% WR). Score distribution shows almost nothing reaches FULL (75–100 band empty; near-55 band tiny) — confidence engine rarely “likes” entries, and when it does, net EV is still ≈ −RT fee.

Orchestrator: `nexus-analyzer/run_all_backtests.py` stages Berserker → Phase4 → **Crypto V3.0 label / V3.1 code** → Scanner.

### Structural fee math (why −2.4% “looks like the fee”)
With default Coinbase Intro modeling:
- RT fee **2.4%** + ~0.1% slip ≈ **2.5%** cost per round trip.  
- BTC recipe: net TP 2.5% ⇒ **gross move must be ~4.9%** before fee-aware TP fires; stop 1.2% ⇒ **~−3.6% net** if stopped.  
- Need WR ≫ 50% with asymmetric payoff, or much larger winners — suite shows WR ~15–23% and avg ≈ −fee.

**Alpaca comparison (if capital moved):** Tier-1 taker 25 bps → **0.50% RT** (~4.8× cheaper than Coinbase Intro). Still does not rescue a −2.4% expectancy signal by itself if the gross edge is near zero; it changes the bar for a *new* strategy class.

### Documented defects / integrity debt (not cheerleading)
1. **Fee vs signal horizon mismatch:** recipes’ net TP ≈ one RT fee; structure pays for noise.  
2. **Jul 11 unlock leak:** FC `/control` sync vs env lock — fixed V5.21; proves unlocks are operationally dangerous without hard env + redeploy.  
3. **Paper contamination of live gates:** V5.5 fixed CONF ratchet trained on ungated paper; buckets still intentionally include paper.  
4. **Data proxy:** BT on Alpaca bars; live on Coinbase microstructure — PatternMemory excludes `bt_` from live gates for this reason.  
5. **POL/SUI empty** in Sunday fetch (`MATIC/USD` / SUI mapping) — universe half-blind on those names.  
6. **Revival plan honesty:** banked 48h pooled mean was **−0.42%** (Jul tape); filters alone don’t create entry edge (`CRYPTO_REVIVAL_PLAN.md`).

### Governance already in force
- `CRYPTO_REVIVAL_PLAN.md` v1.1: only path to flip lock is Phase 1→2→3→4 gates; two dead discovery batches → **SHELVE**.  
- Phase4 equities sealed; **one WR-touching live experiment = BAND_0905 through Oct 15** (`TRAIL_PREREG_Sep13.md` / Berserker trail — not crypto).

---

## 3. Failure modes ranked

| Rank | Mode | Evidence | Why it kills ~small float |
| --- | --- | --- | --- |
| **1. Fee / cost structure** | Dominant | Suite avg ≈ −2.2…−2.6% ≈ modeled RT; V5.10–V5.11 fee truth; Coinbase taker IOC | On $50 notional, ~$1.20 RT fee; a few trades/day erase a thin Coinbase USDC book. Alpaca $295 float is the *equities* survival capital — do not subsidize crypto EV with it. |
| **2. Signal / expectancy** | Co-primary | 15–23% WR; both MR and MOMENTUM negative; external daily MR after 10 bps also dies ([whatdoesntwork.com](https://whatdoesntwork.com/research/one-day-crypto-mean-reversion/)); retail cost studies ([anomiq](https://anomiq.io/blog/mean-reversion-crypto-backtest/), [Mykola-Quant/retail-crypto-alpha](https://github.com/Mykola-Quant/retail-crypto-alpha)) | Predictable move ≪ cost at intraday–daily horizons. |
| **3. Strategy geometry** | Structural | Gross TP must clear 2.4%+; stops are tighter than fee | Classic negative expectancy geometry even if direction were 50/50. |
| **4. Execution / venue** | Material | IOC taker-only; Coinbase precision dust history; Alpaca≠Coinbase bars | Maker path is research aspiration (A7), not live code path. |
| **5. Regime / concentration** | Secondary | D1 KEEP cell concentrated in Jul capitulation week (diagnostics); BTC-vol alts gate | Edges that only print in crash weeks fail OOS / shadow. |
| **6. Operational** | Contained | Jul 11 FC sync clobber | Fixed by env lock; still a reason not to “soft unlock.” |

Overnight gaps / funding: live is **spot**, so no funding paid; funding is used as **filter/signal** (Binance perps as context). Gaps still hurt stops on 24/7 tape.

---

## 4. Paths that could reopen later

Each: mechanism → falsifiable claim → **observation-only first step** → earliest experiment slot **after BAND_0905 (post–Oct 15)** and only if crypto Phase gates allow. **No live unlock recommendations here.**

### A. Finish / grade D1 shadow (already in V5.24)
- **Mechanism:** Multi-day MR; trail-48h ≤ −3.75%; maker-limit sim; 48h hold; funding p90 + hour-20 filters (`CRYPTO_P2_SHADOW_PREREG.md`).  
- **Claim:** n≥25 filled; fill rate ≥40%; filled 48h mean ≥ +1.00% and >0 abs; pos rate ≥60%; edge >0 excluding largest capitulation cluster.  
- **First step:** Analyzer eval script on `crypto_shadow_signals` + Thorn tape (read-only). No Trading-bot edit required for grading.  
- **vs BAND:** Shadow is not WR-touching live equities; may run in parallel. **Live Phase 3/4 crypto still waits for BAND close + crypto Phase gates.**

### B. Venue fee reset (Coinbase → Alpaca spot) — research only for now
- **Mechanism:** Same signal class, RT cost 0.50% taker / 0.30% maker tier-1 ([Alpaca crypto fees](https://docs.alpaca.markets/docs/crypto-fees)).  
- **Claim:** Repriced Sunday suite (swap `CB_TAKER_FEE_PCT`→0.0025 or maker model) flips aggregate avg PnL from ~−2.4% to ≥0 on same recipes — or still fails, proving signal death not just fee.  
- **First step:** Analyzer one-off: fee sensitivity table on existing fingerprint / replay (no live).  
- **vs BAND:** Analyzer-only anytime; **live venue migration is a Matthew Trading-bot project and a new registered experiment after BAND.**

### C. Longer-hold / fewer-trade BTC–ETH only
- **Mechanism:** Collapse universe to BTC/ETH; min hold 24–48h; max 1 concurrent; maker post-only. Matches revival Frame.  
- **Claim:** Fee clearance ≥30% at 1.6% effective maker bar; cell mean >0 absolute and ≥ baseline+0.6% (`CRYPTO_P1_PREREGISTRATION.md` gates).  
- **First step:** Tape thesis script (existing P1 style) restricted to BTC/ETH 48h — analyzer.  
- **vs BAND:** Analyzer now; live only post–BAND + Phase 2–3.

### D. Funding / basis / perps
- **Mechanism:** Funding avoid or basis harvest.  
- **Alpaca:** Spot crypto is GA; **no shorting / no margin on spot**. Perp *trading* is beta/enablement-gated in third-party SDK notes — **not something the current Coinbase spot bot can do**. Funding as **filter from Binance public data** is already in-frame (A4).  
- **Claim:** Top-decile funding block improves 48h clearance by ≥X without destroying n (already partially banked as filter).  
- **First step:** Keep as subtractive filter in tape studies; do **not** build a perps book without a separate venue+compliance decision.  
- **vs BAND:** Filter research anytime; perps book = new program, not a crypto.py flip.

### E. Regime filter / shadow-log-first (Thorn thickening)
- **Mechanism:** Subtractive filters (hour, funding, BTC-vol D4) with no entry claim.  
- **Claim:** Filtered pool 48h mean rises by ≥0.15% vs unfiltered without emptying n.  
- **First step:** Already the Frame; continue passive Thorn.  
- **vs BAND:** Always allowed.

### Honest bottom line for reopen
If Sunday suite stays ~−fee after an Alpaca-fee counterfactual, **crypto won’t clear fees at this architecture** even on cheaper venue — need capital scale (fee tiers + dollar EV), different venue+maker execution, and a strategy whose **gross edge ≫ cost**, not RSI recipe retunes.

---

## 5. Hard no’s (burns capital / violates governance)

1. **Flip `CRYPTO_BUYS_DISABLED` / soft `/buys on` while BAND_0905 is open** — or at all before Phase 4 crypto unlock doc.  
2. **Re-enable live IOC mean-reversion/momentum recipes** that Sunday prices at −2.2…−2.6%/trade.  
3. **“Just trade BTC only” on the same fee-aware recipe** — BTC suite line is still ~−2.3% / 17% WR.  
4. **Raise trade frequency** to “make it back” — fee drag scales with N.  
5. **Fund crypto losses from the ~$295 Alpaca Berserker float.**  
6. **Perps / short / leverage on Alpaca spot** — not available as used today.  
7. **Second WR-touching live experiment** alongside BAND_0905.  
8. **Trading-bot drive-by edits by Fuzzy** — research lives in `nexus-analyzer`.  
9. **Treat paper WR or fee-blind BT as unlock evidence** (pre–V3.1 / pre–V5.10 failure mode).  
10. **Revisit killed families** (F&G bottoms, dip-recovery, BTC lead/lag, etc.) without a new mechanism (`CRYPTO_REVIVAL_PLAN.md`).

---

## 6. Recommended next 1–2 concrete analyzer actions

*(Scripts/docs Fuzzy can write in `nexus-analyzer`. No Trading-bot edits. No live unlock.)*

### Action 1 — `crypto_sunday_lock_memo.py` + short doc (this week)
- **What:** Idempotent reader that pulls latest `crypto_trade_fingerprints` where `trade_id like 'bt_%'` (or scrapes last orchestrator summary) and emits a one-page markdown: per-pair WR, avg net PnL, MR vs MOMENTUM, fee assumption (`CB_TAKER_FEE_PCT`), POL/SUI empty flag.  
- **Also:** Fee **counterfactual** column: recompute avg PnL adding back `(0.024 - alpaca_rt)` to illustrate venue sensitivity without claiming edge.  
- **Why:** Gives Matthew a durable artifact every Sunday instead of chat archaeology; reinforces lock with numbers.  
- **Falsify:** If a future Sunday prints aggregate avg PnL > 0 **net of current fee model**, escalate to human review (still no auto-unlock).

### Action 2 — `crypto_d1_shadow_grade.py` (observation-only)
- **What:** Implement the pre-registered Phase 2 grader in `CRYPTO_P2_SHADOW_PREREG.md`: join `crypto_shadow_signals` → Thorn tape → fill-within-30m → 48h forward from fill; print fill rate, mean, pos rate, cluster jackknife.  
- **Why:** D1 is the only banked entry-shaped KEEP; grading it is the honest path. Pass/fail binds; fail → D1 dies or re-queues per batch clock — **lock unchanged either way**.  
- **Timing:** Can run while BAND_0905 is open (no live WR touch). Do **not** schedule crypto paper/live Phase 3 until after Oct 15 BAND verdict **and** shadow KEEP.

---

## Appendix — Key citations

| Item | Path / URL |
| --- | --- |
| Live engine | `Trading-bot/crypto.py` V5.24 (`CRYPTO_BUYS_DISABLED`, `RECIPES`, `_calc_trade_size`, `try_enter`, D1 shadow) |
| Control API | `Trading-bot/control_server_crypto.py` |
| Cap coord (no crypto) | `Trading-bot/capital_coordinator.py` |
| Sunday orchestrator | `nexus-analyzer/run_all_backtests.py` |
| Fee-aware BT | `nexus-analyzer/crypto_backtester.py` V3.1 |
| Revival / P1 / P2 | `CRYPTO_REVIVAL_PLAN.md`, `CRYPTO_P1_PREREGISTRATION.md`, `CRYPTO_P2_SHADOW_PREREG.md` |
| BAND constraint | `TRAIL_PREREG_Sep13.md` (Berserker; Oct 15 window) |
| Alpaca spot fees / constraints | https://docs.alpaca.markets/docs/crypto-fees · https://docs.alpaca.markets/docs/crypto-trading (spot; no margin/short on crypto spot) |
| External cost evidence | https://whatdoesntwork.com/research/one-day-crypto-mean-reversion/ · https://anomiq.io/blog/mean-reversion-crypto-backtest/ · https://github.com/Mykola-Quant/retail-crypto-alpha |
| Railway | `rare-perception`: `nexus-crypto`, `nexus-backtester` (cron Sunday 23:00 UTC) |

---

*End of report. Observation-only. Lock stays.*
