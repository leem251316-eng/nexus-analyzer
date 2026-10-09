# PRE-REGISTRATION — FOMC-decision and CPI-release day filter (Berserker)

Status: **DRAFT — pending Matthew's review, dated Sep 27 2026.**
Not binding until Matthew accepts it. Column names below are from his
Oct 9 2026 read-only schema check. Stored `kind` strings remain
**TO CONFIRM: kind values.** No `event_tape` row has been read. This file
deploys nothing and writes nothing.
Sibling, filed the same day: `EVENT_POST_8K_PREREG.md`. The two are one family.

## Hypothesis

Berserker is the only live equity book: the 12-symbol universe on the
~$295 Alpaca float (`CRYPTO_DEEP_DIVE_Sep24.md`; symbols in
`nexus_analyzer_1min_railway.py` `TRUMP_THEME` + `TECH_GROWTH`, and the
same list in `fill_truth.py`). Entries on FOMC-decision days and on
CPI-release days have worse fee-cleared per-trade expectancy than entries
on other sessions in the same window. If that gap is large enough, a day
filter could help. The earned object, if any, is that filter. It is not
a buy signal.

## Mechanism

A scheduled macro print reprices the whole book in one window, and
Berserker's entry is a same-day momentum trigger (the V10.19 signal the
backtester copies). On a print day that trigger fires into the repricing
rather than into ordinary drift. CPI for a given reference month is
released at 08:30 ET, before any Berserker entry (the live book is behind
the 08:30–09:00 CT wall). An FOMC decision is released at 14:00 ET
(13:00 CT), so a day filter also drops the morning of that session. The
claim is still the day, because a day filter is what would ship. A
pre-statement vs post-statement split is report-only and cannot move the
gate.

No expectancy by event day has been computed. There is no discovery cell.
The 0.20 pp gap below is the same expectancy-gap size `BAND_0905_PREREG_Sep24.md`
registered ("at least 0.20%/trade below"), not a number taken off this tape.

## Claim

On the frozen window, mean fee-cleared per-trade pnl on FOMC-decision
sessions and CPI-release sessions pooled is at least 0.20 percentage points
worse than on the other sessions in that window, the event-day mean is
negative, and the other-session mean is not.

## Data sources

### Trades (columns found in this repo)

Table `berserker_trade_fingerprints`. One row per `trade_id`.

| column | where it is read |
| --- | --- |
| `symbol`, `entry_ts`, `pnl_pct`, `trade_id`, `is_paper`, `won` | `entry_minute_census.py` (live filter and expectancy = mean `pnl_pct`); `trail_prereg_sim.py`; `opening_delay_verdict.py` |
| `pnl_pct_broker` | `band_verdict.py` header: `COALESCE(pnl_pct_broker, pnl_pct)` when the column exists (Sep 24 fill audit), else `pnl_pct`. `fill_truth_write.py`: "`pnl_pct` is NEVER touched." |

`entry_ts` is epoch seconds. Session date is that instant in
`America/Chicago`, the clock `entry_minute_census.py` and
`berserker_session_thesis.py` already use. `pnl_pct` is in percent
(`nexus_analyzer_1min_railway.py` writes `round(final_pnl * 100, 3)`).

Live rows only: `is_paper = FALSE`, `trade_id NOT LIKE 'bt_%'`,
`won IS NOT NULL`. Symbols: CLSK, MARA, PLTR, GEO, CXW, NUE, MSTR, NVDA,
TSLA, AAPL, SMCI, SPCX. Paper-trial names stay out.

### `event_tape` (columns confirmed Oct 9 2026)

The repo still has no `CREATE TABLE` for `event_tape` (searched at
`74f8cbf`). Matthew ran a read-only schema check on the live database.
Columns, in ordinal order:

| column | type | role in this registration |
| --- | --- | --- |
| `id` | bigint | Row identity. Not a gate. |
| `ts` | bigint | Insert time, epoch seconds. Not the event clock. |
| `event_ts` | bigint | Event time, epoch seconds. Same unit as `berserker_trade_fingerprints.entry_ts` and `exit_ts`. The only clock. |
| `kind` | character varying | Discriminator. **TO CONFIRM: kind values** for an FOMC decision and a CPI release. |
| `symbol` | character varying | Issuer ticker. Macro rows (FOMC/CPI) presumably have `symbol` NULL and are told apart by `kind`. A non-NULL macro `symbol` must not be joined onto a Berserker name. |
| `source` | character varying | Not a gate. Printed beside `kind` in the census below. |
| `ref` | text | Not a gate. |
| `uniq_key` | text | Not a gate. |

Sentry lives in Trading-bot, which this registration does not open and
does not modify. The schema check returned names and types only. The
public Fed and BLS calendars are not a substitute label: a session enters
the treatment set only when an `event_tape` row's `kind` says so. A
release the tape missed is a coverage hole. It is reported and left
unlabeled. It is not filled in from the website.

Sentry V1.1 booted Sep 6 2026. Its 21-day observation gate opened Sep 27
2026. That is why the window starts at boot and stops before today.

This document uses the FOMC-decision `kind` and the CPI-release `kind`
only, once those strings are filled in. The 8-K value is the sibling's.
No other `kind` is a gate.

Before review, Matthew runs this one read-only check and writes the
strings back into this draft:

```
SELECT kind, source, count(*) FROM event_tape GROUP BY 1,2
```

That statement returns counts by `kind` and `source`. It does not join
trades and it does not compute pnl.

There is no form column and no item-code column. `ts` is insert time.
Session assignment uses `event_ts` only. `id`, `source`, `ref`, and
`uniq_key` are not gates.

`event_ts` is bigint epoch seconds, so it has a time of day. Session
assignment, in `America/Chicago`:

- CT time before 15:00 CT: the session is that CT calendar date.
- CT time at or after 15:00 CT: the session is the next NYSE session.
  The print landed after the cash close.
- Date only, no time of day: the session is that calendar date if it is
  an NYSE session; otherwise the next NYSE session. This branch exists
  so a date-typed column has a rule before anyone sees a row. It is not
  a choice made after the read. Confirmed type is bigint epoch seconds,
  so this branch does not apply.

FOMC and CPI are one pooled cell. Splitting them is report-only.

Public calendar, context for sample size only, not labels: the August
CPI printed Friday Sep 11 2026 at 08:30 ET, and the September FOMC
decision printed Wednesday Sep 16 2026 at 14:00 ET. Both sit inside the
first window if Sentry wrote them. The next FOMC decision (Oct 27–28)
does not. The September CPI prints Oct 14 2026, inside the extension
window only.

## Sample

n = closed live trades, equal-weighted, not distinct (session, symbol)
and not dollars.

First window (frozen now):

- Events: tape rows with `event_ts` from 2026-09-06 00:00 CT inclusive
  through 2026-09-27 00:00 CT exclusive. Boot day through the day before
  the gate opened. Sep 6 is a Sunday, so the equity sample still starts
  Tue Sep 8.
- Trades: `entry_ts` session date from Tue Sep 8 2026 through Fri Sep 25
  2026 inclusive. Mon Sep 7 2026 is Labor Day and contributes no session
  (`opening_delay_verdict.py`). Weekends contribute none.

Treatment: entry session is an FOMC-decision session or a CPI-release
session under the rules above.

Control: every other in-window session.

Excluded from both cells (counted in an appendix, not in n, not in either
mean):

- The trade also falls in the sibling's post-8-K window, `(event_ts,
  event_ts + 48h]` on that symbol.
- The trade falls in the sibling's anticipation window, `[event_ts - 48h,
  event_ts]` on that symbol. That window is not a registered thesis.

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

- `n_event` >= 20
- `n_control` >= 40
- distinct treatment sessions >= 2

Ridge-evaluable when at least 3 symbols each have >= 3 treatment trades.
Until then the breadth test cannot be read.

KEEP, only if every line holds:

- floors met and ridge-evaluable
- mean `pnl_pct_net` on treatment < 0
- mean `pnl_pct_net` on control >= 0
- control mean minus treatment mean >= 0.20 percentage points
- breadth: at least 3 symbols show a treatment mean below that symbol's
  own in-window control mean

KILL, when floors are met and the ridge is evaluable, if any KEEP line
fails. A positive event-day mean is a KILL even when the gap is wide: the
filter would turn away trades that still clear the fee. A negative control
mean is a KILL: ordinary days are the problem, and this day filter does
not answer that. A gap under 0.20 pp is a KILL: it is inside four fee
haircuts and below the gap this family borrowed from BAND_0905. A sample
that is ridge-evaluable but fails breadth is a KILL: one or two names
carried a day effect.

EXTEND, when a floor is unmet or the ridge is not yet evaluable. One
extension only. The added tape is 2026-09-27 00:00 CT through 2026-10-18
00:00 CT exclusive (21 more calendar days). Added sessions run through
Fri Oct 16 2026. Mon Oct 12 2026 is Columbus Day; if the NYSE is closed,
that date contributes no session. Confirm the holiday against the exchange
calendar at the read. Do not invent the session. The second read uses the
pooled window, same rules, same bars. Still short, or still not
ridge-evaluable: EXTEND is exhausted. Record NO VERDICT. That is not a
KILL and not a KEEP. This document does not open a third window.

## Multiple comparisons

Family `EVENT_FAMILY_2026-09-27` has two members: this file and
`EVENT_POST_8K_PREREG.md`. Both were written before any `event_tape` read.

- Two primary tests. FOMC vs CPI is not a third. 8-K item codes are not
  a third. PPI, NFP, and the earnings-date blackout are not gates.
- Both verdicts are reported. A KEEP here is not "macro days hurt" if the
  sibling is a KILL, and the sibling is not dropped from the record.
- Trades that qualify for both treatments are in neither primary mean, so
  one trade cannot pass two gates.
- The 0.20 pp bar is an economic bar, not a p-value, and it is not halved
  or doubled for a Bonferroni correction. Multiplicity is controlled by
  the frozen cells, the overlap exclusion, and the rule that neither KEEP
  deploys from this document.
- Whichever members KEEP, they become filter candidates inside one later
  registration. They do not become two live experiments.

## Hard constraints

- A KEEP can only ever become an entry filter. It cannot become a buy
  signal, a size increase, an exit change, or a reason to add a symbol.
- Nothing in `event_tape` is read by a live service. This registration
  does not change that. No live path, in this repo or in Trading-bot,
  gains a reader because this draft exists.
- `BAND_0905` currently owns the single live win-rate-touching slot.
  Its verdict is after the Thu Oct 15 2026 close (`BAND_0905_PREREG_Sep24.md`:
  session 1 = Fri Sep 25, 15th session = Thu Oct 15). Any KEEP from this
  family queues behind that verdict. It does not run beside it, and it
  does not move the Oct 15 date.

## What this does not claim

- Nothing about the first meeting day when no decision is released.
- Nothing about minutes, the press conference as its own event, PPI, or
  the employment situation.
- Nothing per symbol as a shippable exception. The breadth test exists so
  a thin name cannot pass as a day effect.
- No verdict script, and no `event_tape` read other than the kind/source
  census in the data section, is authorized while this status line still
  says DRAFT.
