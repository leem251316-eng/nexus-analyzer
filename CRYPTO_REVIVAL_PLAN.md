# NEXUS CRYPTO REVIVAL PLAN — v1.1 (Jul 29 2026, gates ratified)

Governs the only path to flipping CRYPTO_BUYS_DISABLED. Lock stays until
Phase 4 gates clear. Written against the banked ledger through Jul 29.

---

## Evidence base (banked)

**What survived:**
- **48h horizon** — fee clearance 18.7% pooled vs ~1.4% at 4h (A1, KEEP).
  Caveat: pooled 48h mean is −0.42% on this month's tape — any entry
  signal must beat a NEGATIVE baseline, not zero.
- **Maker route** — median spread 1.9bps; clearance 17.0% vs the 1.6%
  haircutted bar (A7, KEEP). Contrarian entries only — fills cluster on
  adverse moves, so momentum-style maker entries are excluded by design.
- **Funding avoid-filter** — top-decile funding → −0.58% at 24h, 33.1%
  pos, n=4,503 (A4, KEEP per registration wording; script's printed KILL
  checks the wrong tail). Block longs when funding is top-decile.
- **Hour filter** — 9AM CDT best / 8PM CDT worst, replicated. Filter
  only, never a standalone clock trade.

**What died (do not revisit without a NEW mechanism):** F&G bottoms,
dip-recovery/confirmation family (incl. pullback sub-claim), session
edge at 24h, funding contrarian-LONG, liquidation snap-back, BTC
lead/lag (A6, killed clean Jul 29 post units-fix).

**Parked observation (unregistered):** BOTH BTC 5m-move tails
underperform flat (up-tail −0.23%/37.4% pos, down-tail −0.11%/42.8%,
flat −0.01%/49.1%). Shape = "BTC short-term vol → alt underperformance"
avoid-filter. Registers as D4 below; not banked until it passes.

**Missing piece:** the entry edge itself. Everything above is frame,
not signal.

---

## The Frame (every future thesis lives inside this)

1. Hold horizon 24–48h. 4h-scale is structurally dead under fees.
2. Execution modeled post-only maker. Contrarian entries only.
3. Filters stacked at zero cost in every test: funding top-decile
   blocked, worst hours blocked, (D4 if it passes).
4. Fee bar in every verdict: **1.6% effective** (maker + 25%
   adverse-selection haircut). Report 2.4% taker as robustness line.
5. Verdict bar is **baseline-relative**: conditioned cell must beat the
   pooled same-horizon baseline, not zero.

---

## Phase 0 — Instrumentation (this week, zero risk, lock untouched)

- Thorn tape thickens passively (~60K obs / 26 days now; 48h self-join
  coverage grows daily). No crypto.py changes needed — all Phase 1
  theses are computable from existing fields via self-joins (multi-day
  lookbacks, realized vol, day-of-week all derivable from ts + price).
- Fix A4's printed verdict line to match registration (cosmetic).
- Write the Phase 1 pre-registration (below) BEFORE the run date.
- V5.22 paper cooldown verification closes on next red day
  (independent track, must be closed before Phase 3).

**Exit criteria:** pre-registration committed to repo; run date set.

## Phase 1 — Entry-edge discovery (tape-only, target ~Aug 12)

Run date chosen so 48h coverage spans 40+ days. Max FOUR theses per
batch — multiple-comparisons discipline. Candidates:

- **D1. Multi-day mean reversion** — 48h forward conditioned on
  trailing-48h return deciles. Mechanism: multi-day overreaction; loser:
  late momentum chasers. Contrarian → maker-compatible.
- **D2. Slow RSI extremes** — multi-hour RSI (computed from tape prices,
  not rsi_5m) oversold cells vs baseline at 48h.
- **D3. Cross-pair relative strength** — alt's trailing-48h return minus
  BTC's; extreme divergence → convergence. Contrarian pairs logic.
- **D4. BTC-vol avoid-filter** — the parked Jul 29 observation,
  registered fresh: both |btc_ret_5m|>0.15% tails underperform flat at
  4h AND 24h by >=0.15%.

**KEEP gate per thesis (RATIFIED Jul 29 2026, with floor amendment):**
selected cell's 48h mean >= pooled baseline + 0.6%, AND cell mean > 0
ABSOLUTE (a cell that merely loses less than the market does not bank),
AND fee clearance >= 30% in-cell, AND n >= 300, AND monotone across the
conditioning variable (no cherry-picked middle cell). Placebo: shuffled
conditioning. Full text: CRYPTO_P1_PREREGISTRATION.md (binds).

**Program kill clause (data honesty):** two consecutive discovery
batches with zero KEEPs → crypto formally SHELVED. Lock becomes
indefinite; Thorn tape keeps recording; revisit only on a genuinely new
mechanism class (new data field, new market structure), not a re-cut of
killed families.

**Exit criteria:** >=1 KEEP with ratified gates.

## Phase 2 — Shadow mode (2–3 weeks live tape, no orders)

- KEEP thesis coded as shadow signals in crypto.py (V10.61 pattern:
  alert + shadow-table write, zero order paths). Lock untouched.
- Maker-fill simulation on live tape: shadow entry = limit at signal
  bid; counted FILLED only if price touches the limit within the window.
  This tests A7's haircut assumption against reality.

**Gates:** shadow n >= 25 filled; realized 48h forward on filled shadows
>= registered edge − 0.2% tolerance; fill rate >= 40% (below that the
maker economics don't exist and the thesis returns to Phase 1 with the
taker bar).

## Phase 3 — Paper validation

- Paper crypto leg running the full new logic, ISOLATED per the paper-
  isolation learning (own capacity scope; no shared-account
  contamination — the voided Berserker control does not get a sequel).
- V5.22 cooldown verification must be closed before this phase starts.

**Gates:** n >= 25 paper round trips; net-of-modeled-maker-fee expectancy
> 0; WR within 8pts of shadow-phase WR (regime drift check).

## Phase 4 — Live unlock (the ONLY path to flipping the var)

Pre-registered unlock doc, then: flip CRYPTO_BUYS_DISABLED + redeploy.
- Micro size (exchange minimum), maker-only, max 1 concurrent position.
- **Auto-relock circuit breaker in code, not policy:** 5 consecutive
  losers OR −4% cumulative → re-lock without human input.
- 30-live-trade checkpoint vs pre-registered bars. Fail → re-lock,
  return to Phase 1. Pass → sizing review.

---

## Timeline (honest)

Phase 1 verdicts ~Aug 12 → shadow through ~Sep 2 → paper through
~Sep 23 → **earliest live unlock mid/late Sept**, and only if every
gate passes first try. Any gate failure recycles, and two dead
discovery batches shelve the program. No date pressure overrides a
gate — the lock has no opinion about the calendar.
