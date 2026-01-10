from __future__ import annotations
import json
from pathlib import Path

def main():
    # Placeholder CLI entrypoint.
    # Implement: load rank file -> rebalance -> compute equity curve -> save plots/reports to artifacts/
    cfg = json.loads(Path("configs/default.json").read_text(encoding="utf-8"))
    top_n = cfg.get("top_n", 5)
    rank_path = Path(cfg["paths"]["signals"]) / f"rank_top{top_n}.parquet"
    print("[run_backtest] expecting rank file at:", rank_path)

if __name__ == "__main__":
    main()
