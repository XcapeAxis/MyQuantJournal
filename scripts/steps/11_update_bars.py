from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import argparse
import re

import pandas as pd
import akshare as ak
from tqdm import tqdm

# ======== 关键修复 1：固定项目根目录（不依赖你从哪里运行） ========
ROOT = Path(__file__).resolve().parents[1]  # scripts/ 的上一级

BARS_DIR = ROOT / "data" / "bars_qfq"
META_DIR = ROOT / "data" / "meta"
BARS_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS_CSV = META_DIR / "symbols.csv"
REGISTRY_CSV = META_DIR / "registry.csv"


def normalize_code(x: str) -> str | None:
    """从任意字符串中提取 6 位数字股票代码。"""
    s = str(x)
    m = re.search(r"(\d{6})", s)
    return m.group(1) if m else None


def is_mainboard(code: str) -> bool:
    """沪深主板：0/6 开头，排除科创板 688/689。"""
    if not code or len(code) != 6:
        return False
    if code[0] not in ("0", "6"):
        return False
    if code.startswith(("688", "689")):
        return False
    return True


class RateLimiter:
    """全局限速：所有线程共享，保证两次请求间隔 >= min_interval 秒。"""

    def __init__(self, min_interval: float = 0.25):
        self.min_interval = float(min_interval)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            dt = now - self._last
            if dt < self.min_interval:
                time.sleep(self.min_interval - dt)
            self._last = time.time()


@dataclass
class UpdateResult:
    code: str
    updated: bool
    last_date: str | None
    error: str | None
    empty: bool = False


def load_symbols(limit: int | None = None) -> list[str]:
    if not SYMBOLS_CSV.exists():
        raise FileNotFoundError(f"Missing {SYMBOLS_CSV}. Run scripts/10_symbols_mainboard.py first.")

    df = pd.read_csv(SYMBOLS_CSV)
    if "code" not in df.columns:
        raise RuntimeError(f"symbols.csv missing 'code' column: {df.columns.tolist()}")

    codes = (
        df["code"].astype(str)
        .map(normalize_code)
        .dropna()
        .astype(str)
        .str.zfill(6)
        .tolist()
    )

    # 主板过滤
    codes = [c for c in codes if is_mainboard(c)]

    # 去重
    seen = set()
    uniq = []
    for c in codes:
        if c not in seen:
            uniq.append(c)
            seen.add(c)

    if limit and limit > 0:
        uniq = uniq[:limit]

    if not uniq:
        raise RuntimeError(
            "No valid symbols loaded.\n"
            f"- Check {SYMBOLS_CSV} content.\n"
            "- If codes contain prefixes like 'sh600519', this script now extracts 6 digits; "
            "if still empty, your symbols file may be blank or not A-shares."
        )

    return uniq


def load_registry(codes: list[str]) -> pd.DataFrame:
    if REGISTRY_CSV.exists():
        reg = pd.read_csv(REGISTRY_CSV, dtype={"code": str})
        reg["code"] = reg["code"].astype(str).str.zfill(6)
    else:
        reg = pd.DataFrame({"code": codes, "last_date": [""] * len(codes)})

    reg = reg.set_index("code").reindex(codes).reset_index()
    reg["last_date"] = reg["last_date"].fillna("")
    return reg


def save_registry(reg: pd.DataFrame) -> None:
    reg.to_csv(REGISTRY_CSV, index=False, encoding="utf-8-sig")


def infer_last_date_from_files(code: str) -> pd.Timestamp | None:
    code_dir = BARS_DIR / f"code={code}"
    if not code_dir.exists():
        return None

    year_dirs = []
    for p in code_dir.glob("year=*"):
        try:
            y = int(p.name.split("=")[1])
            year_dirs.append((y, p))
        except Exception:
            continue
    if not year_dirs:
        return None

    year_dirs.sort(key=lambda x: x[0])
    _, latest_year_dir = year_dirs[-1]
    part = latest_year_dir / "part.parquet"
    if not part.exists():
        return None

    df = pd.read_parquet(part, columns=["date"])
    if df.empty:
        return None
    return pd.to_datetime(df["date"]).max()


def fetch_hist_qfq(
    code: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    limiter: RateLimiter,
    max_retry: int = 3,
) -> pd.DataFrame:
    for attempt in range(max_retry):
        try:
            limiter.wait()
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
            df = df.dropna(subset=["date", "open", "high", "low", "close"]).copy()
            return df[["date", "open", "high", "low", "close", "volume", "openinterest"]]

        except Exception as e:
            time.sleep(0.8 * (attempt + 1))
            last_err = str(e)

    raise RuntimeError(f"fetch failed code={code} start={start_yyyymmdd} end={end_yyyymmdd} err={last_err}")


def append_write_year_partition(code: str, year: int, new_part: pd.DataFrame) -> None:
    out_dir = BARS_DIR / f"code={code}" / f"year={year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part.parquet"

    if out_path.exists():
        old = pd.read_parquet(out_path)
        df = pd.concat([old, new_part], ignore_index=True)
    else:
        df = new_part.copy()

    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(["date"]).sort_values("date").reset_index(drop=True)
    df.to_parquet(out_path, index=False)


def update_one(
    code: str,
    last_date_str: str,
    end_yyyymmdd: str,
    start_if_missing: str,
    limiter: RateLimiter,
) -> UpdateResult:
    try:
        inferred = None
        if last_date_str and str(last_date_str).strip():
            inferred = pd.to_datetime(last_date_str)
        else:
            inferred = infer_last_date_from_files(code)

        if inferred is None:
            start = start_if_missing
        else:
            start = (inferred + pd.Timedelta(days=1)).strftime("%Y%m%d")

        if pd.to_datetime(start) > pd.to_datetime(end_yyyymmdd):
            return UpdateResult(code=code, updated=False, last_date=last_date_str or None, error=None, empty=True)

        new_df = fetch_hist_qfq(code, start, end_yyyymmdd, limiter=limiter)
        if new_df.empty:
            return UpdateResult(code=code, updated=False, last_date=last_date_str or None, error=None, empty=True)

        new_df["year"] = new_df["date"].dt.year.astype(int)
        for y, g in new_df.groupby("year"):
            append_write_year_partition(code, int(y), g.drop(columns=["year"]).reset_index(drop=True))

        last_dt = pd.to_datetime(new_df["date"]).max().strftime("%Y-%m-%d")
        return UpdateResult(code=code, updated=True, last_date=last_dt, error=None, empty=False)

    except Exception as e:
        return UpdateResult(code=code, updated=False, last_date=last_date_str or None, error=str(e), empty=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="只更新前 N 只股票（用于测试）。0=全量")
    ap.add_argument("--workers", type=int, default=6, help="线程数（建议 4~8）")
    ap.add_argument("--min-interval", type=float, default=0.30, help="全局限速间隔（秒）")
    ap.add_argument("--start-if-missing", type=str, default="20160101", help="本地没数据时的起始日 YYYYMMDD")
    ap.add_argument("--end", type=str, default="", help="结束日 YYYYMMDD（空=今天）")

    # ======== 单测：先验证 AKShare 数据源是否可用 ========
    ap.add_argument("--test-symbol", type=str, default="", help="单测一只股票是否能拉到数据，例如 600519")

    args = ap.parse_args()

    end = args.end.strip() or pd.Timestamp.today().strftime("%Y%m%d")

    print(f"[INFO] ROOT={ROOT}")
    print(f"[INFO] BARS_DIR={BARS_DIR}")
    print(f"[INFO] SYMBOLS_CSV={SYMBOLS_CSV} exists={SYMBOLS_CSV.exists()}")

    limiter = RateLimiter(min_interval=args.min_interval)

    if args.test_symbol.strip():
        code = normalize_code(args.test_symbol)
        if not code:
            raise RuntimeError(f"Invalid --test-symbol: {args.test_symbol}")
        df = fetch_hist_qfq(code, args.start_if_missing, end, limiter=limiter)
        print(f"[TEST] code={code} rows={len(df)} start={args.start_if_missing} end={end}")
        if not df.empty:
            out = ROOT / "data" / f"test_{code}_qfq.parquet"
            df.to_parquet(out, index=False)
            print(f"[TEST] Saved sample: {out}")
        return

    codes = load_symbols(limit=args.limit or None)
    print(f"[INFO] loaded symbols={len(codes)} sample={codes[:5]}")

    reg = load_registry(codes)
    last_map = dict(zip(reg["code"].tolist(), reg["last_date"].astype(str).tolist()))

    results: list[UpdateResult] = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [
            ex.submit(update_one, c, last_map.get(c, ""), end, args.start_if_missing, limiter)
            for c in codes
        ]

        for fut in tqdm(as_completed(futs), total=len(futs), desc="Downloading"):
            results.append(fut.result())

    res_map = {r.code: r for r in results}

    upd_cnt = sum(1 for r in results if r.updated)
    err_cnt = sum(1 for r in results if r.error)
    empty_cnt = sum(1 for r in results if (not r.updated and not r.error and r.empty))

    # 写回 registry
    new_last = []
    for _, row in reg.iterrows():
        c = row["code"]
        r = res_map.get(c)
        if r and r.updated:
            new_last.append(r.last_date)
        else:
            new_last.append(row["last_date"])

    reg["last_date"] = new_last
    save_registry(reg)

    print(f"Done. updated={upd_cnt} empty={empty_cnt} errors={err_cnt} registry={REGISTRY_CSV}")

    # 如果全部 empty，给出强提示
    if upd_cnt == 0 and err_cnt == 0 and len(codes) > 0:
        print("[WARN] All requests returned empty data.")
        print("       - Try: python scripts/11_update_bars_incremental_mt.py --test-symbol 600519")
        print("       - If test also empty, AKShare data source may be blocked/unstable on your network.")


if __name__ == "__main__":
    main()