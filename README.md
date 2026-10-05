# SynthEPEL-R75 v2.5 — Context-V2 Focused Measurement

This build implements the current research checklist while keeping the system **measurement-only**.

## Confirmed changes implemented

1. **EPEL_ENFORCE=false**
   - The EPEL gate is advisory for research.
   - This does NOT mean the bot trades raw signals. Trading remains hard-disabled with `ENABLE_TRADING=false`.
   - `EPEL_ENFORCE` is exposed in health/dashboard so a future execution build can distinguish research from enforcement.

2. **HORIZONS_TICKS=5,7**
   - The experiment now focuses on the two most interesting horizons from the current observation set.
   - 3, 10 and 15 ticks are removed from the default experiment to reduce multiple-comparison noise.

3. **EPEL_MIN_SAMPLES=100**
   - The qualification gate is stricter.
   - Wilson 95% lower bound must clear the live proposal-based EPEL threshold after at least 100 prior observations in the selected context.

4. **PROBABILITY_MODE=CONTEXT_V2**
   - Probability is now conditioned on pre-signal market context rather than using one pooled direction-only rate.
   - Context is leakage-safe and uses only information available before the signal:
     - trend bias over the lookback window: `UP`, `DOWN`, or `BALANCED`
     - short-vs-long realized tick-volatility regime: `LOW`, `NORMAL`, or `HIGH`
   - Example context keys: `UP_HIGH`, `BALANCED_NORMAL`, `DOWN_LOW`.
   - Each horizon/direction/context bucket has its own bounded `{wins,total}` counters.
   - A context cannot qualify until it has 100 prior observations.

5. **MIN_MOVE_PCT remains 0.15 by default**
   - The screenshots recommend testing 0.25/0.30 as sensitivity cases, not blindly making either the new default.
   - Therefore the default stays at 0.15 until the data supports changing it.

## What remains unchanged

- Market: `R_75`
- Momentum direction: positive move -> RISE/CALL; negative move -> FALL/PUT
- Lookback: 20 ticks
- Live Deriv proposal pricing: ON
- Dynamic EPEL threshold from actual proposal price/payout
- 95% Wilson lower bound
- Opposite-direction outcome logging
- Memory-safe aggregate counters
- Authenticated REST + OTP WebSocket connection
- **No buy/sell/trade execution**

## Important interpretation

The current screenshots are encouraging, especially the 5- and 7-tick raw momentum results, but they are not proof of a durable edge. The EPEL-qualified samples are still small and overlapping horizons are correlated. This version therefore narrows the experiment and raises the evidence threshold rather than enabling live trading.

`EPEL_LAMBDA=1.00` remains the packaged default. The live proposal price still determines the actual EPEL threshold for each observation.

## Ledger

The new experiment writes to:
`data/r75_tick_epel_ledger_v25.csv`

The ledger now includes `context_key`, `context_mode`, and the number of prior context samples used for the probability estimate.

## Render environment

Use the same credentials as the working SynthTrade bot. Do not send the API token in chat.

Recommended defaults:

- `DERIV_ACCOUNT_TYPE=demo`
- `ASSET=R_75`
- `HORIZONS_TICKS=5,7`
- `LOOKBACK_TICKS=20`
- `MIN_MOVE_PCT=0.15`
- `EPEL_ENABLED=true`
- `EPEL_LAMBDA=1.00`
- `EPEL_MIN_SAMPLES=100`
- `EPEL_ENFORCE=false`
- `PROBABILITY_MODE=CONTEXT_V2`
- `WILSON_Z=1.959964`
- `PROPOSAL_ENABLED=true`
- `PROPOSAL_TIMEOUT_MS=3000`
- `ENABLE_TRADING=false`

## Suggested next research step

Run this unchanged for another substantial sample (target roughly 500 additional signals) before changing `MIN_MOVE_PCT`. Separately, test `0.25` and `0.30` on the historical CSV as sensitivity experiments; do not replace the live default based on an in-sample win-rate jump alone.
