from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_config(config_path: Path) -> dict:
    try:
        return json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Config file not found: {config_path}. Provide --config to set a valid path."
        )


def main() -> None:
    # Placeholder CLI entrypoint.
    # Implement: incremental download/update bars, save to data/raw or data/processed.
    parser = argparse.ArgumentParser(description="Incremental download/update bars")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/default.json"),
        help="Path to config file",
    )
    args = parser.parse_args()

    try:
        cfg = load_config(args.config)
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
        return

    try:
        print(
            "[update_bars] config loaded:",
            cfg["bar_freq"],
            cfg["start_date"],
            cfg["end_date"],
        )
    except KeyError as exc:
        print(f"[ERROR] Missing required config field: {exc}")


if __name__ == "__main__":
    main()
