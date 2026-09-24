# PRE-REGISTRATION — Second opening wall, 09:05–09:30 CT (Sep 24 2026)

Status: DRAFT until committed to nexus-analyzer. Deploys as the ONE
WR-touching experiment in V10.68 (Gate B closed Sep 24, slot open).
Template: the V10.61 opening-delay wall — shadow-logged blocked signals,
Gate A / Gate B, verdict script. That template just produced a clean KEEP;
this reuses it with expectancy gates instead of WR gates.

## Discovery (SEEN — never in the verdict)
entry_minute_census.py V1.0, live trades entered Jul 29–Sep 24, n=310,
41 sessions, all-trade expectancy +0.083%/trade:
  09:00–09:05   n=37  WR 51.4%  exp +0.280   (the queued release at the wall)
  09:05–09:30   n=78  WR 32.1%  exp −0.275   sl ~50%   (pooled from 4 slices)
  09:30–14:30   n=165 (rest of day)          exp ≈ +0.12
The band boundary was chosen after looking. The 09:00–09:05 exemption is
the part most at risk of being a carve; it is kept because it has a
mechanism (below) and because dropping it would block the best 5-minute
slice on the tape.

## Mechanism
The V10.61 wall blocks 08:30–09:00. Signals that are still true at
09:00:00 have persisted through the blocked half-hour and enter in the
first sweeps — momentum that survived the open. Signals that first turn
true 09:05–09:30 are opening-range noise: fresh breakouts inside a range
that has not resolved. Same reasoning as the original wall, one step later.

## Claim
Entries whose first signal falls in 09:05–09:30 CT have expectancy at
least 0.20%/trade below entries outside the band, on a fresh window, net of
0.05% fee/slip.

## Operationalization (main.py V10.68)
In the Berserker entry gate: if 09:05:00 <= now_CT < 09:30:00 and the
signal is true, DO NOT enter; write one row to berserker_shadow_signals
with reason = 'band_0905' (same writer, same verified-write + heartbeat
path as the V10.61 wall rows). Signals still true at 09:30:00 enter
normally. Nothing else changes: the 08:30–09:00 wall stays (KEEP),
exits untouched, sizing untouched.

## Verdict (band_verdict.py — a parameterized copy of opening_delay_verdict.py)
n = distinct (session, symbol), FIRST tick's price per key; raw ticks are
liveness only. Sessions = distinct dates with >= 1 blocked signal; the
deploy day is excluded by declaration if the deploy lands inside RTH.
Floors: 15 sessions AND >= 30 distinct blocked signals.
  Gate A (the block did not hurt what still trades):
    live expectancy of entries outside 09:05–09:30 since deploy
    >= +0.00%/trade, n >= 40.
  Gate B (what was blocked would have lost):
    blocked signals replayed through the live bracket (TP 1.5 / SL 1.0,
    SPCX 1.5; base trail 1.5; MIN_HOLD 20; EOD 14:58; ambiguous bar =
    WIN, anti-validation) -> expectancy <= −0.10%/trade net.
  Ridge (time effect, not a thin-name effect): among symbols with >= 3
    blocked signals, at least 5 show negative simulated expectancy;
    otherwise PARK regardless of A and B.
  KEEP = A and B and ridge.  REVERT = A fails or B fails.  PARK = floors
  or ridge unmet at the verdict date -> extend once by 10 sessions, then
  decide.
Replica note (declared): the minute-bar replay fills exits at the level;
live fills run worse. That flatters the blocked band, which makes Gate B
HARDER to pass — anti-validation, as intended.

## Verdict date
15 sessions after deploy. If V10.68 deploys Mon Sep 28: session 1 = Sep 29,
verdict after the close of Mon Oct 19 (no holidays in the window).

## What is NOT claimed
- Nothing about 14:30–14:58. The census killed the late-entry-cutoff hunch:
  n=14, WR 57%, exp +0.20. Not registered.
- Nothing about overnight carry. Carried trades are the best subset on the
  tape (n=49, WR 65%, exp +0.70, vs 13/17 gap-ups) — PARK-grade, next in
  line after this verdict, registered on its own.
- Nothing per-symbol. MARA/NUE/GEO look worst in the band; the ridge
  requirement is there so a thin-name effect cannot pass as a time effect.
