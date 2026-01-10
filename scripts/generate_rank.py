from __future__ import annotations
import json
from pathlib import Path

def main():
    # Placeholder CLI entrypoint.
    # Implement: read bars/features -> compute scores -> write rank_top{N}.parquet/csv into data/signals/
    cfg = json.loads(Path("configs/default.json").read_text(encoding="utf-8"))
    top_n = cfg.get("top_n", 5)
    out_dir = Path(cfg["paths"]["signals"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"rank_top{top_n}.parquet"
    print("[generate_rank] would write:", out_path)

if __name__ == "__main__":
    main()
