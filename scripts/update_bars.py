from __future__ import annotations
import json
from pathlib import Path

def main():
    # Placeholder CLI entrypoint.
    # Implement: incremental download/update bars, save to data/raw or data/processed.
    cfg = json.loads(Path("configs/default.json").read_text(encoding="utf-8"))
    print("[update_bars] config loaded:", cfg["bar_freq"], cfg["start_date"], cfg["end_date"])

if __name__ == "__main__":
    main()
