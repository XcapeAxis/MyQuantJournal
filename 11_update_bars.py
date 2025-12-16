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
    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_yyyymmdd,
        end_date=end_yyyymmdd,
        adjust="qfq",
    )
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "amount",
    })
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["openinterest"] = 0
    df = df.dropna(subset=["date", "open", "high", "low", "close"])
    return df[["date", "open", "high", "low", "close", "volume", "openinterest"]]


def read_existing_code(code: str) -> pd.DataFrame:
    code_dir = BARS_DIR / f"code={code}"
    if not code_dir.exists():
        return pd.DataFrame()

    parts = list(code_dir.rglob("*.parquet"))
    if not parts:
        return pd.DataFrame()

    dfs = [pd.read_parquet(p) for p in parts]
    df = pd.concat(dfs, ignore_index=True).drop_duplicates(["date"]).sort_values("date")
    return df


def write_partitions(code: str, df: pd.DataFrame):
    """
    简单可靠：重写该 code 全部分区（code/year）。
    初次全量下载用这个方式最稳。
    后续如果你要“只更新当年”，再改这个函数即可。
    """
    df = df.copy()
    df["year"] = df["date"].dt.year.astype(int)

    code_dir = BARS_DIR / f"code={code}"
    code_dir.mkdir(parents=True, exist_ok=True)

    # 清理旧文件
    for p in code_dir.rglob("*.parquet"):
        try:
            p.unlink()
        except Exception:
            pass

    for y, g in df.groupby("year"):
        out_dir = code_dir / f"year={y}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "part.parquet"
        g = g.drop(columns=["year"]).sort_values("date").reset_index(drop=True)
        g.to_parquet(out_path, index=False)


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


def job_one(code: str, last_date: str, start_default: str, end_default: str) -> tuple[str, str, int, str]:
    """
    单股票下载任务（线程执行）
    返回：(code, new_last_date(YYYY-MM-DD or ''), rows_added, status)
    status: 'ok'/'empty'/'fail'
    """
    start = compute_start(last_date, start_default)
    end = end_default

    # 指数退避重试
    for attempt in range(MAX_RETRY):
        try:
            new_df = fetch_hist_qfq(code, start, end)
            time.sleep(BASE_SLEEP + random.random() * JITTER)

            if new_df.empty:
                return code, last_date, 0, "empty"

            old_df = read_existing_code(code)
            merged = pd.concat([old_df, new_df], ignore_index=True)
            merged = merged.drop_duplicates(["date"]).sort_values("date")

            write_partitions(code, merged)

            new_last = merged["date"].max().strftime("%Y-%m-%d")
            rows_added = len(merged) - len(old_df) if not old_df.empty else len(merged)
            return code, new_last, int(rows_added), "ok"

        except Exception as e:
            # 退避：1,2,4,8...
            backoff = (2 ** attempt) + random.random()
            time.sleep(backoff)

    return code, last_date, 0, "fail"


def main():
    ensure_dirs()
    symbols = load_symbols()
    reg = load_registry(symbols)

    # 任务列表
    tasks = [(row["code"], row["last_date"]) for _, row in reg.iterrows()]
    total = len(tasks)

    ok_cnt = 0
    empty_cnt = 0
    fail_cnt = 0
    added_total = 0

    # 用 dict 做快速写回
    last_map = dict(zip(reg["code"], reg["last_date"]))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [
            ex.submit(job_one, code, last_date, START_DEFAULT, END_DEFAULT)
            for code, last_date in tasks
        ]

        for fut in tqdm(as_completed(futures), total=total, desc="Downloading"):
            code, new_last, rows_added, status = fut.result()

            if status == "ok":
                ok_cnt += 1
                added_total += rows_added
                last_map[code] = new_last
            elif status == "empty":
                empty_cnt += 1
            else:
                fail_cnt += 1

    # 写回 registry
    reg["last_date"] = reg["code"].map(last_map).fillna("")
    save_registry(reg)

    print("\n=== Summary ===")
    print(f"symbols: {total}")
    print(f"ok: {ok_cnt}, empty: {empty_cnt}, fail: {fail_cnt}")
    print(f"rows_added_total (rough): {added_total}")
    print(f"registry: {REGISTRY_CSV}")


if __name__ == "__main__":
    main()
