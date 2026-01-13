from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd


def get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def audit_db_coverage(
    project_name: str,
    db_path: Path,
    universe_path: Path,
    output_dir: Path,
    freq: str,
) -> None:
    if not universe_path.exists():
        raise FileNotFoundError(f"Universe file not found: {universe_path}")
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "db_coverage_report.csv"
    output_json = output_dir / "db_coverage_summary.json"

    with open(universe_path, "r", encoding="utf-8") as handle:
        universe_codes = [line.strip() for line in handle if line.strip()]

    print(f"Project: {project_name}")
    print(f"Total universe codes: {len(universe_codes)}")

    coverage_data = []
    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        for code in universe_codes:
            sql = """SELECT COUNT(*), MIN(datetime), MAX(datetime)
                     FROM bars
                     WHERE symbol=? AND freq=?"""
            cursor.execute(sql, (code, freq))
            bars_count, first_date, last_date = cursor.fetchone()
            coverage_data.append(
                {
                    "code": code,
                    "bars_count": bars_count,
                    "first_date": first_date,
                    "last_date": last_date,
                }
            )

    df = pd.DataFrame(coverage_data)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"Saved db_coverage_report.csv to {output_csv}")

    valid_df = df[df["bars_count"] > 0].copy()

    summary = {
        "project": project_name,
        "n_codes_in_universe": len(universe_codes),
        "n_codes_in_db": len(valid_df),
        "median_bars": float(valid_df["bars_count"].median()) if not valid_df.empty else 0.0,
        "p10_bars": float(valid_df["bars_count"].quantile(0.1)) if not valid_df.empty else 0.0,
        "p90_bars": float(valid_df["bars_count"].quantile(0.9)) if not valid_df.empty else 0.0,
        "min_first_date": valid_df["first_date"].min() if not valid_df.empty else None,
        "max_last_date": valid_df["last_date"].max() if not valid_df.empty else None,
    }

    if not valid_df.empty:
        valid_df["date_range_days"] = (
            pd.to_datetime(valid_df["last_date"]) - pd.to_datetime(valid_df["first_date"])
        ).dt.days
        summary["median_date_range_days"] = float(valid_df["date_range_days"].median())
        summary["p10_date_range_days"] = float(valid_df["date_range_days"].quantile(0.1))
        summary["p90_date_range_days"] = float(valid_df["date_range_days"].quantile(0.9))

    with open(output_json, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)

    print(f"Saved db_coverage_summary.json to {output_json}")
    print("\n=== Database Coverage Summary ===")
    print(f"Universe codes: {summary['n_codes_in_universe']}")
    print(f"Codes with data: {summary['n_codes_in_db']}")
    print(f"Median bars: {summary['median_bars']:.1f}")
    print(f"P10 bars: {summary['p10_bars']:.1f}")
    print(f"P90 bars: {summary['p90_bars']:.1f}")
    print(f"Date range: {summary['min_first_date']} to {summary['max_last_date']}")
    if "median_date_range_days" in summary:
        print(f"Median date range: {summary['median_date_range_days']:.1f} days")

    if valid_df.empty:
        print("\n回测实际可用的历史区间：数据库中没有足够的历史数据，无法进行有效回测。")
    else:
        median_days = summary.get("median_date_range_days", 0)
        if median_days < 30:
            print(
                f"\n回测实际可用的历史区间：大多数股票只有 {median_days:.0f} 天数据，回测只能解释为'冒烟测试'。"
            )
        else:
            print(
                f"\n回测实际可用的历史区间：大多数股票有 {median_days:.0f} 天数据，可以进行有效回测。"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit SQLite bars coverage vs universe list.")
    parser.add_argument("--project", type=str, default="2026Q1_mom", help="Project name")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/market.db"),
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--universe-path",
        type=Path,
        default=None,
        help="Path to universe_codes.txt (defaults to project meta directory)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for reports (defaults to project meta directory)",
    )
    parser.add_argument("--freq", type=str, default="1d", help="Bar frequency")
    args = parser.parse_args()

    universe_path = args.universe_path or Path("data/projects") / args.project / "meta" / "universe_codes.txt"
    output_dir = args.output_dir or Path("data/projects") / args.project / "meta"

    audit_db_coverage(
        project_name=args.project,
        db_path=args.db_path,
        universe_path=universe_path,
        output_dir=output_dir,
        freq=args.freq,
    )


if __name__ == "__main__":
    main()
