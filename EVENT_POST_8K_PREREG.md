# PRE-REGISTRATION — Post-8-K entry window (Berserker)

Status: **DRAFT — pending Matthew's review, dated Sep 27 2026.**
Not binding until Matthew accepts it. Column names below are from his
Oct 9 2026 read-only schema check. Stored `kind` strings remain
**TO CONFIRM: kind values.** No `event_tape` row has been read. This file
deploys nothing and writes nothing.
Sibling, filed the same day: `EVENT_FOMC_CPI_PREREG.md`. The two are one family.

## Hypothesis

Berserker is the only live equity book: the 12-symbol universe on the
~$295 Alpaca float (`CRYPTO_DEEP_DIVE_Sep24.md`; symbols in
`nexus_analyzer_1min_railway.py` `TRUMP_THEME` + `TECH_GROWTH`, and the
same list in `fill_truth.py`). An entry in the 48 hours after that same
symbol files an 8-K has worse fee-cleared per-trade expectancy than an
entry with no 8-K on that symbol in the surrounding 48 hours. If that gap
is large enough, a per-symbol entry filter could help. The earned object,
if any, is that filter. It is not a buy signal, and it is not a reason to
buy the filing.

## Mechanism

The live earnings blackout already treats a known company event as a
48-hour block. `nexus_analyzer_1min_railway.py` V3.3:

```
EARNINGS_BUFFER_H   = 48     # hours before/after earnings to block
```

`is_earnings_blocked` returns true when the absolute gap between the bar
and an earnings timestamp is <= 48 hours (lines 209–215). The header says
this matches V10.19 `earnings_blocked` (lines 41–43). That constant is the
only company-event window in this repo. This registration uses its length.

It does not use both sides. The before-leg exists because an earnings date
is on a calendar (`fetch_earnings_dates` via yfinance) and is known before
the print. An 8-K acceptance is not known ahead. Entries before the filing
are a different claim (anticipation). They are excluded from this test.
They are not a second gate.

48 hours, not a same-session cutoff and not a grid: one window, frozen,
so the read cannot pick the horizon after seeing which horizon loses.
Item codes (2.02, 5.02, 7.01, and the rest) stay pooled. A slice by item
is report-only and cannot move the gate.

No post-8-K expectancy has been computed. There is no discovery cell. The
0.20 pp gap below is the same expectancy-gap size
`BAND_0905_PREREG_Sep24.md` registered, not a number taken off this tape.

## Claim

On the frozen window, mean fee-cleared per-trade pnl on entries inside
`(event_ts, event_ts + 48h]` after a same-symbol 8-K is at least 0.20
percentage points worse than on entries outside both that window and the
48 hours before the filing, the post-8-K mean is negative, and the other
mean is not.

## Data sources

### Trades (columns found in this repo)

Table `berserker_trade_fingerprints`. One row per `trade_id`.

| column | where it is read |
| --- | --- |
| `symbol`, `entry_ts`, `pnl_pct`, `trade_id`, `is_paper`, `won` | `entry_minute_census.py`; `trail_prereg_sim.py`; `opening_delay_verdict.py` |
| `pnl_pct_broker` | `band_verdict.py`: `COALESCE(pnl_pct_broker, pnl_pct)` when the column exists, else `pnl_pct`. `fill_truth_write.py`: "`pnl_pct` is NEVER touched." |

`entry_ts` is epoch seconds. Comparisons to the filing clock are absolute
time, then displayed in `America/Chicago`. `pnl_pct` is in percent
(`nexus_analyzer_1min_railway.py` writes `round(final_pnl * 100, 3)`).

Live rows only: `is_paper = FALSE`, `trade_id NOT LIKE 'bt_%'`,
`won IS NOT NULL`. Symbols: CLSK, MARA, PLTR, GEO, CXW, NUE, MSTR, NVDA,
TSLA, AAPL, SMCI, SPCX. The join is exact ticker equality. No subsidiary
mapping, no fuzzy match. A tape symbol outside those 12 does not join.

### `event_tape` (columns confirmed Oct 9 2026)

The repo still has no `CREATE TABLE` for `event_tape` (searched at
`74f8cbf`). Matthew ran a read-only schema check on the live database.
Columns, in ordinal order:

| column | type | role in this registration |
| --- | --- | --- |
| `id` | bigint | Row identity. Not a gate. |
| `ts` | bigint | Insert time, epoch seconds. Not the event clock. |
| `event_ts` | bigint | Event time, epoch seconds. Same unit as `berserker_trade_fingerprints.entry_ts` and `exit_ts`. The acceptance/dissemination clock, not a period-of-report date. The only clock. |
| `kind` | character varying | Discriminator. There is no form column. An 8-K is its own `kind`. **TO CONFIRM: kind values** for the string that means 8-K. |
| `symbol` | character varying | Issuer ticker, exact match to the trade `symbol`. 8-K rows are per-symbol. |
| `source` | character varying | Not a gate. Printed beside `kind` in the census below. |
| `ref` | text | Not a gate. Not an item code. |
| `uniq_key` | text | Not a gate. Not used to merge or split filings. |

Sentry lives in Trading-bot, which this registration does not open and
does not modify. The schema check returned names and types only. EDGAR is
not a second label inside the verdict. A filing enters the treatment set
only when an `event_tape` row's `kind` says it is an 8-K for that
`symbol`. A filing the tape missed is a coverage hole. It is reported
and left unlabeled.

Sentry V1.1 booted Sep 6 2026. Its 21-day observation gate opened Sep 27
2026. Filings accepted before boot are not in this tape and are not
backfilled.

Before review, Matthew runs this one read-only check and writes the
8-K string back into this draft:

```
SELECT kind, source, count(*) FROM event_tape GROUP BY 1,2
```

That statement returns counts by `kind` and `source`. It does not join
trades and it does not compute pnl.

There is no form column and no item-code column. `ts` is insert time.
The window uses `event_ts` only. `source`, `ref`, and `uniq_key` are not
gates.

`event_ts` is bigint epoch seconds, so it has a time of day. Window
clock:

- Treatment is `entry_ts` in `(event_ts, event_ts + 48 hours]`.
  Anticipation, excluded from both cells, is
  `[event_ts - 48 hours, event_ts]`.
- Date only, no time of day. `event_ts` is set to 09:00 CT on the first
  NYSE session that starts strictly after that date. The same 48-hour
  treatment and anticipation intervals then apply. A date-only stamp
  cannot show that the filing-date session was after the filing; starting
  at the next open keeps pre-filing entries out of the treatment set.
  That makes a KEEP harder if the damage was on the filing date itself.
  The branch is pre-declared so it is not picked after the read.
  Confirmed type is bigint epoch seconds, so this branch does not apply.

Overlapping 8-Ks on one symbol: the entry is in treatment if it falls in
any post window. It counts once. The filing id for the breadth and
distinct-filing floors is the earliest filing whose post window covers
that entry.

## Sample

n = closed live trades, equal-weighted.

First window (frozen now), same trade dates as the sibling:

- Filings: `event_ts` from 2026-09-06 00:00 CT inclusive through
  2026-09-27 00:00 CT exclusive. A filing inside that span can cover a
  trade after Fri Sep 25. Those later trades wait for the extension.
  The first trade window does not stretch to follow them.
- Trades: `entry_ts` session date from Tue Sep 8 2026 through Fri Sep 25
  2026 inclusive. Mon Sep 7 2026 is Labor Day and contributes no session
  (`opening_delay_verdict.py`).

Treatment: in the post-8-K window for that symbol, and the entry session
is not an FOMC-decision or CPI-release session under the sibling's rules.

Control: not in any same-symbol post window, not in any same-symbol
anticipation window, and not on an FOMC-decision or CPI-release session.

Excluded from both cells (appendix only, not in n, not in either mean):

- Anticipation-window entries.
- Entries that are both post-8-K and on an FOMC-decision or CPI-release
  session. They are the overlap of the two theses.

An exclusion that drops a cell under its floor is EXTEND. The excluded
trades are not put back.

## Primary metric

Gross per trade:

```
G = COALESCE(pnl_pct_broker, pnl_pct)   -- if pnl_pct_broker exists
G = pnl_pct                             -- otherwise
```

Fee-cleared per trade, the only primary metric:

```
pnl_pct_net = G - 0.05
```

The `0.05` is percentage points, one haircut per trade, the subtraction
`band_verdict.py` applies to mean P&L ("0.05 percentage points (fee/slip)")
and the `0.05%/exit` convention in `TRAIL_PREREG_Sep13.md`. It is not
0.10, and it is not recomputed from the backtester's per-side
`SLIPPAGE_PCT`. `pnl_pct_net` is verdict arithmetic. It is never written
to `pnl_pct`, to `pnl_pct_broker`, or to any other column. `pnl_pct` is
not overwritten.

Cell statistic: the arithmetic mean of `pnl_pct_net`. Win rate is printed
and is not a gate.

## Gates

Floors, all three, on the sample after exclusions:

- `n_post` >= 15
- `n_control` >= 40
- distinct 8-K filings that cover at least one treatment trade >= 4

Fifteen trades is the floor because a single filing can produce several
entries, and four filings is what stops one document from being the
result. Both bind. Neither is a substitute for the other.

Ridge-evaluable when at least 3 symbols each have >= 2 treatment trades.
Until then the breadth test cannot be read.

KEEP, only if every line holds:

- floors met and ridge-evaluable
- mean `pnl_pct_net` on treatment < 0
- mean `pnl_pct_net` on control >= 0
- control mean minus treatment mean >= 0.20 percentage points
- breadth: at least 3 symbols show a treatment mean below that symbol's
  own in-window control mean

KILL, when floors are met and the ridge is evaluable, if any KEEP line
fails. A positive post-8-K mean is a KILL even when the gap is wide. A
negative control mean is a KILL: ordinary entries are the problem. A gap
under 0.20 pp is a KILL. A sample that is ridge-evaluable but fails
breadth is a KILL: one filing name carried it.

EXTEND, when a floor is unmet or the ridge is not yet evaluable. One
extension only. The added tape is 2026-09-27 00:00 CT through 2026-10-18
00:00 CT exclusive. Added sessions run through Fri Oct 16 2026. Mon Oct 12
2026 is Columbus Day; if the NYSE is closed, that date contributes no
session. Confirm the holiday against the exchange calendar at the read.
The second read uses the pooled window, same 48-hour rule, same bars.
Still short, or still not ridge-evaluable: EXTEND is exhausted. Record
NO VERDICT. That is not a KILL and not a KEEP. This document does not
open a third window or a second horizon.

## Multiple comparisons

Family `EVENT_FAMILY_2026-09-27` has two members: this file and
`EVENT_FOMC_CPI_PREREG.md`. Both were written before any `event_tape` read.

- Two primary tests. An 8-K item-code split is not a third test. FOMC and
  CPI are the sibling's one pooled cell, not two more. PPI, NFP, and the
  earnings-date blackout are not gates here; the 48-hour length is
  borrowed from that blackout, and the earnings-date rule itself is not
  re-opened.
- Both verdicts are reported. A KEEP here is not "filings hurt" if the
  sibling is a KILL, and the sibling is not dropped from the record.
- Trades that qualify for both treatments are in neither primary mean.
- The 0.20 pp bar is an economic bar, not a p-value, and it is not halved
  or doubled for a Bonferroni correction. Multiplicity is controlled by
  the frozen cells, the overlap exclusion, and the rule that neither KEEP
  deploys from this document.
- Whichever members KEEP, they become filter candidates inside one later
  registration. They do not become two live experiments.

## Hard constraints

- A KEEP can only ever become an entry filter. It cannot become a buy
  signal, a size increase, an exit change, or a reason to add a symbol.
  Blocking the 48 hours after an 8-K is the entire action a KEEP can
  justify. It does not justify buying the 8-K, fading it, or holding
  through it.
- Nothing in `event_tape` is read by a live service. This registration
  does not change that. No live path, in this repo or in Trading-bot,
  gains a reader because this draft exists.
- `BAND_0905` currently owns the single live win-rate-touching slot.
  Its verdict is after the Thu Oct 15 2026 close (`BAND_0905_PREREG_Sep24.md`:
  session 1 = Fri Sep 25, 15th session = Thu Oct 15). Any KEEP from this
  family queues behind that verdict. It does not run beside it, and it
  does not move the Oct 15 date.

## What this does not claim

- Nothing about the 48 hours before the filing.
- Nothing about a horizon other than 48 hours. A same-day or next-open
  cutoff is not registered, even if the appendix shows one.
- Nothing by 8-K item. The pool is the claim.
- Nothing that promotes the existing earnings-date blackout, or that
  retires it. That rule is already live and is not under test here.
- No verdict script, and no `event_tape` read other than the kind/source
  census in the data section, is authorized while this status line still
  says DRAFT.
