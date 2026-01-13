from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def check_rank_date(rank_path: Path) -> None:
    if not rank_path.exists():
        raise FileNotFoundError(f"Rank file not found: {rank_path}")
    df = pd.read_parquet(rank_path)
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Number of dates: {len(df['date'].unique())}")
    print(f"Number of rows: {len(df)}")
    print(f"Unique codes: {len(df['code'].unique())}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Check rank parquet date range.")
    parser.add_argument(
        "--rank-path",
        type=Path,
        default=Path("data/projects/2026Q1_mom/signals/rank_top5.parquet"),
        help="Path to rank parquet file",
    )
    args = parser.parse_args()
    check_rank_date(args.rank_path)


if __name__ == "__main__":
    main()
