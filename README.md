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

2. Run pipeline (example):
   - `python scripts/update_bars.py --config configs/default.json`
   - `python scripts/generate_rank.py --config configs/default.json`
   - `python scripts/run_backtest.py --config configs/default.json`

## Project Structure
- `src/quant_mvp/` core library
- `scripts/` CLI entrypoints
- `configs/` experiment configs (tracked)
- `data/` local datasets (NOT tracked except placeholders)
- `artifacts/` outputs (NOT tracked)
- `docs/` design notes / methodology
- `tests/` unit tests
