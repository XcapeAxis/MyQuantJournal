import pandas as pd
import sqlite3
from pathlib import Path


def get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def main():
    # 配置路径
    project_name = "2026Q1_mom"
    universe_path = Path(f"d:/我的/quant_mvp/data/projects/{project_name}/meta/universe_codes.txt")
    db_path = Path("d:/我的/quant_mvp/data/market.db")
    output_csv = Path(f"d:/我的/quant_mvp/data/projects/{project_name}/meta/db_coverage_report.csv")
    output_json = Path(f"d:/我的/quant_mvp/data/projects/{project_name}/meta/db_coverage_summary.json")
    freq = "1d"

    # 读取universe代码
    with open(universe_path, "r") as f:
        universe_codes = [line.strip() for line in f if line.strip()]
    
    print(f"Total universe codes: {len(universe_codes)}")

    # 查询每个股票的覆盖情况
    coverage_data = []
    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        for code in universe_codes:
            # 查询该股票的bars数量、最早日期和最晚日期
            sql = """SELECT COUNT(*), MIN(datetime), MAX(datetime) 
                     FROM bars 
                     WHERE symbol=? AND freq=?"""
            cursor.execute(sql, (code, freq))
            bars_count, first_date, last_date = cursor.fetchone()
            coverage_data.append({
                "code": code,
                "bars_count": bars_count,
                "first_date": first_date,
                "last_date": last_date
            })
    
    # 创建DataFrame并保存到CSV
    df = pd.DataFrame(coverage_data)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"Saved db_coverage_report.csv to {output_csv}")

    # 生成摘要统计
    # 只考虑有数据的股票
    valid_df = df[df["bars_count"] > 0]
    
    summary = {
        "n_codes_in_universe": len(universe_codes),
        "n_codes_in_db": len(valid_df),
        "median_bars": float(valid_df["bars_count"].median()),
        "p10_bars": float(valid_df["bars_count"].quantile(0.1)),
        "p90_bars": float(valid_df["bars_count"].quantile(0.9)),
        "min_first_date": valid_df["first_date"].min() if not valid_df.empty else None,
        "max_last_date": valid_df["last_date"].max() if not valid_df.empty else None
    }
    
    # 计算实际可用的历史区间
    if not valid_df.empty:
        # 计算每个股票的历史区间长度
        valid_df["date_range_days"] = pd.to_datetime(valid_df["last_date"]) - pd.to_datetime(valid_df["first_date"])
        valid_df["date_range_days"] = valid_df["date_range_days"].dt.days
        
        # 添加到摘要
        summary["median_date_range_days"] = float(valid_df["date_range_days"].median())
        summary["p10_date_range_days"] = float(valid_df["date_range_days"].quantile(0.1))
        summary["p90_date_range_days"] = float(valid_df["date_range_days"].quantile(0.9))
    
    # 保存摘要到JSON
    import json
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"Saved db_coverage_summary.json to {output_json}")
    
    # 打印关键信息
    print(f"\n=== Database Coverage Summary ===")
    print(f"Universe codes: {summary['n_codes_in_universe']}")
    print(f"Codes with data: {summary['n_codes_in_db']}")
    print(f"Median bars: {summary['median_bars']:.1f}")
    print(f"P10 bars: {summary['p10_bars']:.1f}")
    print(f"P90 bars: {summary['p90_bars']:.1f}")
    print(f"Date range: {summary['min_first_date']} to {summary['max_last_date']}")
    if "median_date_range_days" in summary:
        print(f"Median date range: {summary['median_date_range_days']:.1f} days")
    
    # 明确回答回测实际可用的历史区间
    if valid_df.empty:
        print("\n回测实际可用的历史区间：数据库中没有足够的历史数据，无法进行有效回测。")
    else:
        # 计算有效历史区间（大多数股票都有数据的区间）
        # 这里简化处理，使用中位数日期范围
        median_days = summary.get("median_date_range_days", 0)
        if median_days < 30:
            print(f"\n回测实际可用的历史区间：大多数股票只有 {median_days:.0f} 天数据，回测只能解释为'冒烟测试'。")
        else:
            print(f"\n回测实际可用的历史区间：大多数股票有 {median_days:.0f} 天数据，可以进行有效回测。")


if __name__ == "__main__":
    main()
