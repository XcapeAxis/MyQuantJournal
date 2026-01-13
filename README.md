# Quant ML MVP

A minimal, reproducible pipeline for:
- data ingestion (incremental bars)
- feature engineering
- signal/ranking generation (e.g., top-N)
- portfolio backtest (rebalancing)
- evaluation metrics & plots

## Quickstart
1. Create venv and install deps:
   - `python -m venv .venv`
   - Windows: `.venv\Scripts\activate`
   - `pip install -r requirements.txt`

2. Run pipeline (example entrypoints):
   - `python scripts/update_bars.py --config configs/default.json`
   - `python scripts/generate_rank.py --config configs/default.json`
   - `python scripts/run_backtest.py --config configs/default.json`

3. Run the step-based workflow (project-oriented):
   - `python scripts/steps/10_symbols.py --project 2026Q1_mom`
   - `python scripts/steps/11_update_bars.py --mode incremental`
   - `python scripts/steps/20_make_weights_mom.py --project 2026Q1_mom`
   - `python scripts/steps/30_bt_rebalance.py --project 2026Q1_mom --no-show`

4. Utilities and audits:
   - `python scripts/tools/audit_db_coverage.py --project 2026Q1_mom --db-path data/market.db`
   - `python scripts/tools/check_rank_date.py --rank-path data/projects/2026Q1_mom/signals/rank_top5.parquet`

## Project Structure
- `src/quant_mvp/` core library
- `scripts/` CLI entrypoints
  - `scripts/steps/` pipeline steps
  - `scripts/examples/` standalone demos
  - `scripts/tools/` audits and utilities
  - `scripts/benchmarks/` performance checks
  - `scripts/legacy/` historical scripts kept for reference
- `configs/` experiment configs (tracked)
- `data/` local datasets (NOT tracked except placeholders)
- `artifacts/` outputs (NOT tracked)
- `docs/` design notes / methodology
- `tests/` unit tests
