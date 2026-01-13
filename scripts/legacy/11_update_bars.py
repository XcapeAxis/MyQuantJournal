from __future__ import annotations

import os
import time
import random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import akshare as ak
from tqdm import tqdm

# ----------------------------
# 配置区
# ----------------------------
START_DEFAULT = "20160101"
END_DEFAULT = "20251201" 

MAX_WORKERS = 10
MAX_RETRY = 5
BASE_SLEEP = 0.10          # 成功请求后的小睡（秒），越小越快，但越容易限流
JITTER = 0.20              # 抖动（秒），防止请求过于规律

DATA_DIR = Path("data")
BARS_DIR = DATA_DIR / "bars_qfq"
META_DIR = DATA_DIR / "meta"
SYMBOLS_CSV = META_DIR / "symbols.csv"
REGISTRY_CSV = META_DIR / "registry.csv"


# ----------------------------
# 工具函数
# ----------------------------
def ensure_dirs():
    BARS_DIR.mkdir(parents=True, exist_ok=True)
    META_DIR.mkdir(parents=True, exist_ok=True)


def get_symbols() -> pd.DataFrame:
    """
    获取全A股票列表，并过滤北交所，仅保留 0/3/6 开头。
    保存到 data/meta/symbols.csv
    """
    df = ak.stock_info_a_code_name()
    df.columns = [c.lower() for c in df.columns]

    # 兼容不同列名
    if "code" not in df.columns:
        code_col = None
        for c in df.columns:
            if "代码" in c or "code" in c:
                code_col = c
                break
        if code_col is None:
            raise ValueError(f"Cannot find code column in {df.columns.tolist()}")
        df = df.rename(columns={code_col: "code"})

    if "name" not in df.columns:
        for c in df.columns:
            if "名称" in c or "name" in c:
                df = df.rename(columns={c: "name"})
                break

    df["code"] = df["code"].astype(str).str.zfill(6)

    # 过滤：沪深A
    df = df[df["code"].str[0].isin(["0", "3", "6"])].drop_duplicates("code")
    out = df[["code"] + (["name"] if "name" in df.columns else [])].reset_index(drop=True)

    out.to_csv(SYMBOLS_CSV, index=False, encoding="utf-8-sig")
    return out


def load_symbols() -> list[str]:
    if not SYMBOLS_CSV.exists():
        get_symbols()
    df = pd.read_csv(SYMBOLS_CSV, dtype={"code": str})
    return df["code"].astype(str).str.zfill(6).tolist()


def load_registry(symbols: list[str]) -> pd.DataFrame:
    """
    registry 记录每只股票最后更新到哪天（YYYY-MM-DD）
    """
    if REGISTRY_CSV.exists():
        reg = pd.read_csv(REGISTRY_CSV, dtype={"code": str})
        reg["code"] = reg["code"].astype(str).str.zfill(6)
        reg["last_date"] = reg["last_date"].fillna("").astype(str)
    else:
        reg = pd.DataFrame({"code": symbols, "last_date": [""] * len(symbols)})

    reg = reg.set_index("code").reindex(symbols).reset_index()
    reg["last_date"] = reg["last_date"].fillna("").astype(str)
    return reg


def save_registry(reg: pd.DataFrame):
    reg.to_csv(REGISTRY_CSV, index=False, encoding="utf-8-sig")


def fetch_hist_qfq(code: str, start_yyyymmdd: str, end_yyyymmdd: str) -> pd.DataFrame:
    """从AKShare获取前复权历史数据，并进行数据验证和清理"""
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_yyyymmdd,
            end_date=end_yyyymmdd,
            adjust="qfq",
        )
        if df is None or df.empty:
            return pd.DataFrame()

        # 兼容不同版本的列名
        column_mapping = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if "日期" in col or col_lower == "date":
                column_mapping[col] = "date"
            elif "开盘" in col or col_lower == "open":
                column_mapping[col] = "open"
            elif "最高" in col or col_lower == "high":
                column_mapping[col] = "high"
            elif "最低" in col or col_lower == "low":
                column_mapping[col] = "low"
            elif "收盘" in col or col_lower == "close":
                column_mapping[col] = "close"
            elif "成交量" in col or col_lower == "volume":
                column_mapping[col] = "volume"

        df = df.rename(columns=column_mapping)
        
        # 验证必要列是否存在
        required_cols = ["date", "open", "high", "low", "close"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            print(f"Warning: {code} missing columns: {missing_cols}")
            return pd.DataFrame()

        # 转换日期格式
        try:
            df["date"] = pd.to_datetime(df["date"])
        except Exception as e:
            print(f"Warning: {code} date conversion failed: {e}")
            return pd.DataFrame()
        
        df = df.sort_values("date")

        # 转换数值类型
        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # 添加openinterest列（如果不存在）
        if "openinterest" not in df.columns:
            df["openinterest"] = 0

        # 清理无效数据
        df = df.dropna(subset=["date", "open", "high", "low", "close"])
        
        # 验证数据有效性
        if df.empty:
            return pd.DataFrame()
        
        # 确保返回的列存在
        result_cols = ["date", "open", "high", "low", "close"]
        if "volume" in df.columns:
            result_cols.append("volume")
        if "openinterest" in df.columns:
            result_cols.append("openinterest")
        
        return df[result_cols]
    except Exception as e:
        print(f"Error fetching {code}: {e}")
        return pd.DataFrame()


def read_existing_code(code: str) -> pd.DataFrame:
    """读取已存在的股票数据，包含错误处理"""
    code_dir = BARS_DIR / f"code={code}"
    if not code_dir.exists():
        return pd.DataFrame()

    try:
        parts = list(code_dir.rglob("*.parquet"))
        if not parts:
            return pd.DataFrame()

        # 逐个读取文件，忽略损坏的文件
        dfs = []
        for p in parts:
            try:
                df_part = pd.read_parquet(p)
                if not df_part.empty and "date" in df_part.columns:
                    dfs.append(df_part)
            except Exception as e:
                print(f"Warning: Failed to read {p}: {e}")
                continue

        if not dfs:
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)
        
        # 验证date列存在
        if "date" not in df.columns:
            print(f"Warning: {code} data missing date column")
            return pd.DataFrame()
        
        # 转换date列为datetime类型（如果还不是）
        if not pd.api.types.is_datetime64_any_dtype(df["date"]):
            try:
                df["date"] = pd.to_datetime(df["date"])
            except Exception as e:
                print(f"Warning: {code} date conversion failed: {e}")
                return pd.DataFrame()
        
        df = df.drop_duplicates(["date"]).sort_values("date")
        return df
    except Exception as e:
        print(f"Error reading existing data for {code}: {e}")
        return pd.DataFrame()


def write_partitions(code: str, df: pd.DataFrame):
    """
    简单可靠：重写该 code 全部分区（code/year）。
    初次全量下载用这个方式最稳。
    后续如果你要"只更新当年"，再改这个函数即可。
    
    包含完整的错误处理和事务性写入（先写临时文件再重命名）
    """
    if df.empty:
        return
    
    # 验证必要列
    if "date" not in df.columns:
        raise ValueError(f"{code}: DataFrame missing 'date' column")
    
    df = df.copy()
    
    # 确保date是datetime类型
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    
    df["year"] = df["date"].dt.year.astype(int)

    code_dir = BARS_DIR / f"code={code}"
    code_dir.mkdir(parents=True, exist_ok=True)

    # 清理旧文件（记录失败的文件，最后报告）
    failed_deletes = []
    for p in code_dir.rglob("*.parquet"):
        try:
            p.unlink()
        except Exception as e:
            failed_deletes.append((p, str(e)))

    # 写入新分区（使用临时文件确保原子性）
    written_files = []
    try:
        for y, g in df.groupby("year"):
            out_dir = code_dir / f"year={y}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / "part.parquet"
            temp_path = out_path.with_suffix(".tmp")
            
            try:
                g_clean = g.drop(columns=["year"]).sort_values("date").reset_index(drop=True)
                # 先写入临时文件
                g_clean.to_parquet(temp_path, index=False)
                # 原子性重命名
                temp_path.replace(out_path)
                written_files.append(out_path)
            except Exception as e:
                # 清理失败的临时文件
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except Exception:
                        pass
                raise Exception(f"Failed to write year={y} for {code}: {e}")
        
        # 如果所有写入成功，报告删除失败的文件（如果有）
        if failed_deletes:
            print(f"Warning: {code} - {len(failed_deletes)} old files could not be deleted")
    except Exception as e:
        # 如果写入失败，清理已写入的文件
        for f in written_files:
            try:
                f.unlink()
            except Exception:
                pass
        raise


def compute_start(last_date_str: str, start_default: str) -> str:
    """
    last_date_str: 'YYYY-MM-DD' or ''
    返回 yyyymmdd
    """
    if not last_date_str or last_date_str.strip() == "":
        return start_default
    try:
        dt = pd.to_datetime(last_date_str)
        dt2 = dt + pd.Timedelta(days=1)
        return dt2.strftime("%Y%m%d")
    except Exception:
        return start_default


def job_one(code: str, last_date: str, start_default: str, end_default: str) -> tuple[str, str, int, str, str]:
    """
    单股票下载任务（线程执行）
    返回：(code, new_last_date(YYYY-MM-DD or ''), rows_added, status, error_msg)
    status: 'ok'/'empty'/'fail'
    error_msg: 错误信息（如果status为'fail'）
    """
    start = compute_start(last_date, start_default)
    end = end_default
    last_error = None

    # 指数退避重试
    for attempt in range(MAX_RETRY):
        try:
            new_df = fetch_hist_qfq(code, start, end)
            time.sleep(BASE_SLEEP + random.random() * JITTER)

            if new_df.empty:
                return code, last_date, 0, "empty", ""

            # 读取现有数据
            old_df = read_existing_code(code)
            
            # 合并数据
            if old_df.empty:
                merged = new_df.copy()
            else:
                # 验证两个DataFrame的列是否兼容
                if not set(new_df.columns).issubset(set(old_df.columns)):
                    missing_cols = set(new_df.columns) - set(old_df.columns)
                    raise ValueError(f"Column mismatch: {missing_cols} missing in old data")
                merged = pd.concat([old_df, new_df], ignore_index=True)
            
            merged = merged.drop_duplicates(["date"]).sort_values("date")

            # 写入分区（如果失败会抛出异常）
            write_partitions(code, merged)

            # 计算新增行数
            new_last = merged["date"].max().strftime("%Y-%m-%d")
            rows_added = len(merged) - len(old_df) if not old_df.empty else len(merged)
            return code, new_last, int(rows_added), "ok", ""

        except Exception as e:
            last_error = str(e)
            # 退避：1,2,4,8...
            backoff = (2 ** attempt) + random.random()
            time.sleep(backoff)

    # 所有重试都失败
    error_msg = f"{code}: {last_error}" if last_error else f"{code}: Unknown error"
    return code, last_date, 0, "fail", error_msg


def main():
    """主函数，包含完整的错误处理和统计"""
    try:
        ensure_dirs()
        symbols = load_symbols()
        if not symbols:
            print("Error: No symbols loaded")
            return
        
        reg = load_registry(symbols)

        # 任务列表
        tasks = [(row["code"], row["last_date"]) for _, row in reg.iterrows()]
        total = len(tasks)

        if total == 0:
            print("Error: No tasks to process")
            return

        ok_cnt = 0
        empty_cnt = 0
        fail_cnt = 0
        added_total = 0
        failed_codes = []

        # 用 dict 做快速写回
        last_map = dict(zip(reg["code"], reg["last_date"]))

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [
                ex.submit(job_one, code, last_date, START_DEFAULT, END_DEFAULT)
                for code, last_date in tasks
            ]

            for fut in tqdm(as_completed(futures), total=total, desc="Downloading"):
                try:
                    code, new_last, rows_added, status, error_msg = fut.result()

                    if status == "ok":
                        ok_cnt += 1
                        added_total += rows_added
                        last_map[code] = new_last
                    elif status == "empty":
                        empty_cnt += 1
                    else:
                        fail_cnt += 1
                        if error_msg:
                            failed_codes.append(error_msg)
                except Exception as e:
                    fail_cnt += 1
                    print(f"Error processing result: {e}")

        # 写回 registry（包含错误处理）
        try:
            reg["last_date"] = reg["code"].map(last_map).fillna("")
            save_registry(reg)
        except Exception as e:
            print(f"Warning: Failed to save registry: {e}")

        # 打印统计信息
        print("\n=== Summary ===")
        print(f"symbols: {total}")
        print(f"ok: {ok_cnt}, empty: {empty_cnt}, fail: {fail_cnt}")
        print(f"rows_added_total (rough): {added_total}")
        print(f"registry: {REGISTRY_CSV}")
        
        # 打印失败详情（如果有）
        if failed_codes:
            print(f"\nFailed codes (first 10):")
            for err in failed_codes[:10]:
                print(f"  {err}")
            if len(failed_codes) > 10:
                print(f"  ... and {len(failed_codes) - 10} more")
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
