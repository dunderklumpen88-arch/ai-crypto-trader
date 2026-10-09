# Trader Intelligence patch (paper trading only)

## What this patch does
- Adds an isolated `trader_intelligence.py` module.
- Defaults to disabled and does not place orders.
- Allows checking whether configured public HTTPS JSON sources are reachable.
- Does **not** guess at unknown provider formats: reachable data is marked `reachable_unparsed`.
- Only accepts normalized trader signals for scoring when the track record is marked verified, has at least 20 samples, is fresh, has quality >= 0.55 and max drawdown <= 25%.
- Includes two basic safety tests.

## Important limitation
This is a safe foundation, not a complete live trader leaderboard integration. A provider-specific adapter must be implemented after choosing a source with documented public API access. No trading decisions should be influenced by unparsed data.

## Add to GitHub
1. Open your repository `dunderklumpen88-arch/ai-crypto-trader`.
2. Add `trader_intelligence.py` at the repository root and paste the file contents.
3. Add `test_trader_intelligence.py` at the repository root and paste the test contents.
4. Commit the files.
5. Leave `TRADER_INTEL_ENABLED` unset (disabled) until a source adapter and tests are ready.

## Run tests
`python -m unittest test_trader_intelligence.py`

## Next integration steps
1. Inspect the current `main.py` and paper trading loop.
2. Add a read-only status endpoint for Trader Intelligence.
3. Select a documented public trader-data source and implement its adapter.
4. Persist paper trades to SQLite, include fees/slippage, and test strategy out-of-sample.
5. Do not enable live orders until paper results and risk limits have been independently reviewed.
