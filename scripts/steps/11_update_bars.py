from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import argparse
import re
import sqlite3

import pandas as pd
import akshare as ak
from tqdm import tqdm


# =============================================================================
# Robust repo root discovery
# =============================================================================

def find_repo_root(start: Path) -> Path:
    start = start.resolve()
    for p in [start, *start.parents]:
        if (p / ".git").exists() or (p / "pyproject.toml").exists():
            return p
    # Fallback (should almost never happen)
    return start.parents[2]


ROOT = find_repo_root(Path(__file__))

# Meta files (CSV)
META_DIR = ROOT / "data" / "meta"
META_DIR.mkdir(parents=True, exist_ok=True)
SYMBOLS_CSV = META_DIR / "symbols.csv"
REGISTRY_CSV = META_DIR / "registry.csv"

# SQLite bars database (single file, shared across projects)
DB_PATH = ROOT / "data" / "market.db"


def normalize_code(x: str) -> str | None:
    """Extract 6-digit A-share code from any string."""
    s = str(x)
    m = re.search(r"(\d{6})", s)
    return m.group(1) if m else None


def is_mainboard(code: str) -> bool:
    """CN mainboard: starts with 0/6, excluding STAR 688/689."""
    if not code or len(code) != 6:
        return False
    if code[0] not in ("0", "6"):
        return False
    if code.startswith(("688", "689")):
        return False
    return True


class RateLimiter:
    """Global limiter shared by all threads."""

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
    rows: int = 0
    df: pd.DataFrame | None = None


# =============================================================================
# SQLite helpers
# =============================================================================

def _get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    # Pragmas for ingestion performance & fewer lock issues
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db() -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bars (
                symbol TEXT NOT NULL,
                freq TEXT NOT NULL,
                datetime TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                adj_factor REAL,
                PRIMARY KEY (symbol, freq, datetime)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_bars_symbol_freq_dt ON bars(symbol, freq, datetime);"
        )
        conn.commit()


def infer_last_date_from_db(code: str, freq: str) -> pd.Timestamp | None:
    init_db()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT MAX(datetime) FROM bars WHERE symbol=? AND freq=?",
            (code, freq),
        ).fetchone()
    if not row or not row[0]:
        return None
    try:
        return pd.to_datetime(row[0])
    except Exception:
        return None


def upsert_bars_to_db(code: str, freq: str, df: pd.DataFrame) -> int:
    """Upsert bars into SQLite using ON CONFLICT."""
    if df is None or df.empty:
        return 0

    init_db()

    d = df.copy()
    if "date" not in d.columns:
        raise RuntimeError(f"bars df missing 'date' column: {d.columns.tolist()}")

    d["datetime"] = pd.to_datetime(d["date"]).dt.strftime("%Y-%m-%d")
    for c in ["open", "high", "low", "close", "volume"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    if "adj_factor" not in d.columns:
        # For qfq-adjusted OHLC, adj_factor is not strictly required.
        d["adj_factor"] = 1.0

    d = d.dropna(subset=["datetime", "open", "high", "low", "close"]).copy()
    d = d.drop_duplicates(subset=["datetime"]).sort_values("datetime")

    rows = [
        (
            code,
            freq,
            r["datetime"],
            float(r["open"]) if pd.notna(r["open"]) else None,
            float(r["high"]) if pd.notna(r["high"]) else None,
            float(r["low"]) if pd.notna(r["low"]) else None,
            float(r["close"]) if pd.notna(r["close"]) else None,
            float(r["volume"]) if pd.notna(r["volume"]) else None,
            float(r["adj_factor"]) if pd.notna(r["adj_factor"]) else 1.0,
        )
        for _, r in d.iterrows()
    ]

    if not rows:
        return 0

    sql = """
    INSERT INTO bars(symbol, freq, datetime, open, high, low, close, volume, adj_factor)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(symbol, freq, datetime) DO UPDATE SET
        open=excluded.open,
        high=excluded.high,
        low=excluded.low,
        close=excluded.close,
        volume=excluded.volume,
        adj_factor=excluded.adj_factor
    """

    with _get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()

    return len(rows)


# =============================================================================
# Symbols & registry
# =============================================================================

def _extract_code_column(df: pd.DataFrame) -> pd.Series:
    # Common possibilities
    for col in ["code", "代码", "证券代码", "股票代码"]:
        if col in df.columns:
            return df[col]
    # Fallback: first column
    return df.iloc[:, 0]


def init_symbols_csv(force: bool = False) -> None:
    """Create symbols.csv using AKShare if missing (or if force=True)."""
    if SYMBOLS_CSV.exists() and not force:
        return

    # ak.stock_info_a_code_name() usually returns columns like: code, name
    df = ak.stock_info_a_code_name()
    if df is None or df.empty:
        raise RuntimeError("AKShare returned empty symbols list (stock_info_a_code_name).")

    codes = (
        _extract_code_column(df)
        .astype(str)
        .map(normalize_code)
        .dropna()
        .astype(str)
        .str.zfill(6)
    )

    df_out = pd.DataFrame({"code": codes})
    df_out = df_out[df_out["code"].map(is_mainboard)].drop_duplicates(subset=["code"]).reset_index(drop=True)

    if df_out.empty:
        raise RuntimeError("Failed to build mainboard symbols list from AKShare output.")

    df_out.to_csv(SYMBOLS_CSV, index=False, encoding="utf-8-sig")


def load_symbols(limit: int | None = None, auto_init: bool = True) -> list[str]:
    if not SYMBOLS_CSV.exists():
        if auto_init:
            init_symbols_csv(force=False)
        else:
            raise FileNotFoundError(
                f"Missing {SYMBOLS_CSV}. Please generate it first (or run with --init-symbols)."
            )

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

    codes = [c for c in codes if is_mainboard(c)]

    seen = set()
    uniq: list[str] = []
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
            "- If codes contain prefixes like 'sh600519', this script extracts 6 digits; "
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


# =============================================================================
# AKShare fetch
# =============================================================================

def fetch_hist_qfq(
    code: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    limiter: RateLimiter,
    max_retry: int = 3,
) -> pd.DataFrame:
    last_err = ""
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

            df = df.rename(
                columns={
                    "日期": "date",
                    "开盘": "open",
                    "最高": "high",
                    "最低": "low",
                    "收盘": "close",
                    "成交量": "volume",
                    "成交额": "amount",
                }
            )

            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")

            for c in ["open", "high", "low", "close", "volume"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            df = df.dropna(subset=["date", "open", "high", "low", "close"]).copy()
            return df[["date", "open", "high", "low", "close", "volume"]]

        except Exception as e:
            last_err = str(e)
            time.sleep(0.8 * (attempt + 1))

    raise RuntimeError(
        f"fetch failed code={code} start={start_yyyymmdd} end={end_yyyymmdd} err={last_err}"
    )


def update_one_fetch(
    code: str,
    last_date_str: str,
    freq: str,
    end_yyyymmdd: str,
    start_if_missing: str,
    limiter: RateLimiter,
) -> UpdateResult:
    """Fetch-only worker. DB write is done in main thread to avoid SQLite locking."""
    try:
        inferred_from_registry: pd.Timestamp | None = None
        if last_date_str and str(last_date_str).strip():
            try:
                inferred_from_registry = pd.to_datetime(last_date_str)
            except Exception:
                inferred_from_registry = None

        inferred_from_db = infer_last_date_from_db(code, freq)

        if inferred_from_registry is None:
            inferred = inferred_from_db
        elif inferred_from_db is None:
            inferred = inferred_from_registry
        else:
            inferred = max(inferred_from_registry, inferred_from_db)

        if inferred is None:
            start = start_if_missing
        else:
            start = (inferred + pd.Timedelta(days=1)).strftime("%Y%m%d")

        if pd.to_datetime(start) > pd.to_datetime(end_yyyymmdd):
            return UpdateResult(
                code=code,
                updated=False,
                last_date=(inferred.strftime("%Y-%m-%d") if inferred is not None else None),
                error=None,
                empty=True,
                rows=0,
                df=None,
            )

        new_df = fetch_hist_qfq(code, start, end_yyyymmdd, limiter=limiter)
        if new_df.empty:
            return UpdateResult(
                code=code,
                updated=False,
                last_date=(inferred.strftime("%Y-%m-%d") if inferred is not None else None),
                error=None,
                empty=True,
                rows=0,
                df=None,
            )

        last_dt = pd.to_datetime(new_df["date"]).max().strftime("%Y-%m-%d")
        return UpdateResult(
            code=code,
            updated=True,
            last_date=last_dt,
            error=None,
            empty=False,
            rows=len(new_df),
            df=new_df,
        )

    except Exception as e:
        return UpdateResult(
            code=code,
            updated=False,
            last_date=(last_date_str or None),
            error=str(e),
            empty=False,
            rows=0,
            df=None,
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="Only update first N symbols for testing. 0=all")
    ap.add_argument("--workers", type=int, default=6, help="Threads for fetching (suggest 4~8)")
    ap.add_argument("--min-interval", type=float, default=0.30, help="Global rate limit interval (seconds)")
    ap.add_argument("--start-if-missing", type=str, default="20160101", help="Start date if missing (YYYYMMDD)")
    ap.add_argument("--end", type=str, default="", help="End date (YYYYMMDD). Empty=today")
    ap.add_argument("--freq", type=str, default="1d", help="Frequency label stored in DB (default: 1d)")

    ap.add_argument("--test-symbol", type=str, default="", help="Test one symbol, e.g. 600519")
    ap.add_argument(
        "--init-symbols",
        action="store_true",
        help="Force (re)generate data/meta/symbols.csv via AKShare before updating",
    )
    ap.add_argument(
        "--no-auto-init-symbols",
        action="store_true",
        help="If symbols.csv is missing, do NOT auto-generate; raise error instead",
    )

    args = ap.parse_args()

    end = args.end.strip() or pd.Timestamp.today().strftime("%Y%m%d")
    freq = args.freq.strip() or "1d"

    init_db()

    print(f"[INFO] ROOT={ROOT}")
    print(f"[INFO] DB_PATH={DB_PATH}")
    print(f"[INFO] SYMBOLS_CSV={SYMBOLS_CSV} exists={SYMBOLS_CSV.exists()}")

    limiter = RateLimiter(min_interval=args.min_interval)

    if args.init_symbols:
        init_symbols_csv(force=True)

    if args.test_symbol.strip():
        code = normalize_code(args.test_symbol)
        if not code:
            raise RuntimeError(f"Invalid --test-symbol: {args.test_symbol}")

        df = fetch_hist_qfq(code, args.start_if_missing, end, limiter=limiter)
        n = upsert_bars_to_db(code, freq, df)
        last_dt = pd.to_datetime(df["date"]).max().strftime("%Y-%m-%d") if not df.empty else None
        print(f"[TEST] code={code} rows_fetched={len(df)} rows_upserted={n} last_date={last_dt}")
        return

    codes = load_symbols(limit=args.limit or None, auto_init=(not args.no_auto_init_symbols))
    print(f"[INFO] loaded symbols={len(codes)} sample={codes[:5]}")

    reg = load_registry(codes)
    last_map = dict(zip(reg["code"].tolist(), reg["last_date"].astype(str).tolist()))

    results: list[UpdateResult] = []

    # Fetch in threads; write to DB in main thread on completion to avoid SQLite locking.
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [
            ex.submit(update_one_fetch, c, last_map.get(c, ""), freq, end, args.start_if_missing, limiter)
            for c in codes
        ]

        for fut in tqdm(as_completed(futs), total=len(futs), desc="Fetching"):
            r = fut.result()

            if r.error is None and r.updated and r.df is not None and not r.df.empty:
                try:
                    n_upsert = upsert_bars_to_db(r.code, freq, r.df)
                    r.rows = n_upsert
                except Exception as e:
                    r.error = f"DB write failed: {e}"
                    r.updated = False

            r.df = None
            results.append(r)

    res_map = {r.code: r for r in results}

    upd_cnt = sum(1 for r in results if r.updated)
    err_cnt = sum(1 for r in results if r.error)
    empty_cnt = sum(1 for r in results if (not r.updated and not r.error and r.empty))

    # Write back registry
    new_last: list[str] = []
    for _, row in reg.iterrows():
        c = row["code"]
        r = res_map.get(c)
        if r and r.updated and r.last_date:
            new_last.append(r.last_date)
        else:
            new_last.append(str(row["last_date"]))

    reg["last_date"] = new_last
    save_registry(reg)

    print(
        f"Done. updated={upd_cnt} empty={empty_cnt} errors={err_cnt} "
        f"registry={REGISTRY_CSV} db={DB_PATH}"
    )

    if upd_cnt == 0 and err_cnt == 0 and len(codes) > 0:
        print("[WARN] All requests returned empty data.")
        print("       - Try: python scripts/steps/11_update_bars.py --test-symbol 600519")
        print("       - If test also empty, AKShare may be blocked/unstable on your network.")


if __name__ == "__main__":
    main()
