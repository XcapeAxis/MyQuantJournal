from __future__ import annotations

"""A-share local backtest: 5-day rebalance, compare TopN=1..5 equity curves.

This script is designed to be *self-healing* for common path / missing-file issues:
- Works whether this file sits in <root>/scripts or directly in <root>.
- If rank file (data/signals/rank_top5.parquet) is missing, it can auto-build a simple momentum Top5 rank file
  from your local bars under data/bars_qfq.

Expected local bars layout:
  data/bars_qfq/code=600519/year=2024/part.parquet

Run:
  python 30_bt_rebalance.py

Optional:
  python 30_bt_rebalance.py --auto-build-rank 1 --lookback 20 --rebalance-every 5
  python 30_bt_rebalance.py --save data/signals/topn_1_5.png
"""

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import backtrader as bt
import matplotlib.pyplot as plt
import pandas as pd


# ---------------- Root / path helpers ----------------

def detect_project_root() -> Path:
    """Best-effort project root detection (robust to file placement + working directory)."""
    here = Path(__file__).resolve()

    # Common: <root>/scripts/*.py
    if here.parent.name.lower() == "scripts":
        return here.parent.parent

    # Common: <root>/*.py
    if (here.parent / "scripts").exists() or (here.parent / "data").exists():
        return here.parent

    # Walk up
    for p in [here.parent] + list(here.parents):
        if (p / "scripts").exists() or (p / "data").exists():
            return p

    return here.parent


def is_mainboard(code: str) -> bool:
    code = str(code).zfill(6)
    if code[0] not in ("0", "6"):
        return False
    if code.startswith(("688", "689")):
        return False
    return True


def list_local_codes(bars_dir: Path) -> List[str]:
    if not bars_dir.exists():
        return []
    codes = []
    for p in bars_dir.glob("code=*"):
        if not p.is_dir():
            continue
        code = p.name.split("=")[-1]
        if len(code) == 6 and code.isdigit() and is_mainboard(code):
            codes.append(code)
    return sorted(set(codes))


def load_code_df(
    bars_dir: Path,
    code: str,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
    columns: List[str] | None = None,
) -> pd.DataFrame:
    """Load one symbol's bars from partitioned parquet into a Backtrader-ready df (datetime index)."""
    code = str(code).zfill(6)
    code_dir = bars_dir / f"code={code}"
    parts = sorted(code_dir.rglob("*.parquet"))
    if not parts:
        return pd.DataFrame()

    cols = columns or ["date", "open", "high", "low", "close", "volume", "openinterest"]
    frames = []
    for p in parts:
        try:
            frames.append(pd.read_parquet(p, columns=cols))
        except Exception:
            frames.append(pd.read_parquet(p))

    df = pd.concat(frames, ignore_index=True)
    if df.empty or "date" not in df.columns:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(["date"]).sort_values("date")

    if start is not None:
        df = df[df["date"] >= start]
    if end is not None:
        df = df[df["date"] <= end]

    df = df.set_index("date")

    # If only close requested
    if columns is not None and columns == ["date", "close"]:
        if "close" not in df.columns:
            return pd.DataFrame()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])
        return df[["close"]]

    # Ensure required columns exist
    for c in ["open", "high", "low", "close"]:
        if c not in df.columns:
            return pd.DataFrame()

    if "volume" not in df.columns:
        df["volume"] = 0
    if "openinterest" not in df.columns:
        df["openinterest"] = 0

    for c in ["open", "high", "low", "close", "volume", "openinterest"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])
    return df[["open", "high", "low", "close", "volume", "openinterest"]]


def locate_rank_file(explicit: Path, root: Path) -> Path | None:
    if explicit.exists():
        return explicit

    expected = root / "data" / "signals" / "rank_top5.parquet"
    if expected.exists():
        return expected

    # Common mis-locations (caused by running scripts from a different cwd)
    common = [
        Path.cwd() / "data" / "signals" / "rank_top5.parquet",
        Path.cwd() / "signals" / "rank_top5.parquet",
        root / "scripts" / "data" / "signals" / "rank_top5.parquet",
    ]
    for p in common:
        if p.exists():
            return p

    # Targeted search only in a few roots
    search_roots = []
    for r in [root, root / "scripts", Path.cwd(), Path.cwd() / "scripts"]:
        if r.exists() and r.is_dir():
            search_roots.append(r)

    cands: List[Path] = []
    for r in dict.fromkeys(search_roots):
        try:
            cands.extend(list(r.rglob("rank_top5.parquet")))
        except Exception:
            continue

    if not cands:
        return None

    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]


# ---------------- Rank auto-builder (momentum Top5) ----------------

def pick_reference_calendar(bars_dir: Path, codes: List[str], min_len: int = 260) -> pd.DatetimeIndex:
    """Pick a code that has a reasonably long history to serve as the rebalance date calendar."""
    best = None
    best_len = -1

    for c in codes[:200]:  # cap scan
        df = load_code_df(bars_dir, c, columns=["date", "close"])
        if df.empty:
            continue
        n = len(df)
        if n > best_len:
            best_len = n
            best = df.index
        if best_len >= min_len:
            break

    if best is None or best_len < 50:
        raise RuntimeError("No local code has enough bars to build a trading calendar.")

    return pd.DatetimeIndex(best).sort_values()


def build_rank_top5_from_local(
    bars_dir: Path,
    out_path: Path,
    lookback: int = 20,
    rebalance_every: int = 5,
    topk: int = 5,
    min_bars: int = 160,
) -> Path:
    codes = list_local_codes(bars_dir)
    if not codes:
        raise FileNotFoundError(f"No local bars found under: {bars_dir}")

    cal = pick_reference_calendar(bars_dir, codes)
    if len(cal) <= lookback + 5:
        raise RuntimeError("Trading calendar too short.")

    reb_dates = cal[lookback::rebalance_every]

    rows: List[pd.DataFrame] = []
    for code in codes:
        s = load_code_df(bars_dir, code, columns=["date", "close"])
        if s.empty or len(s) < min_bars:
            continue

        close = s["close"].astype(float)
        mom = close.pct_change(lookback)
        mom_reb = mom.reindex(reb_dates).dropna()
        if mom_reb.empty:
            continue

        df = mom_reb.reset_index()
        df.columns = ["date", "score"]
        df["code"] = code
        rows.append(df)

    if not rows:
        raise RuntimeError("No momentum scores built. Check your local bars coverage.")

    scores = pd.concat(rows, ignore_index=True)
    scores = scores.sort_values(["date", "score"], ascending=[True, False])
    scores["rank"] = scores.groupby("date")["score"].rank(method="first", ascending=False)
    scores = scores[scores["rank"] <= topk].copy()
    scores["rank"] = scores["rank"].astype(int)
    scores["code"] = scores["code"].astype(str).str.zfill(6)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    scores.to_parquet(out_path, index=False)
    return out_path


# ---------------- Backtrader components ----------------

class ChinaStockComm(bt.CommInfoBase):
    """Simplified A-share costs: commission on both sides + stamp duty on sells."""

    params = (
        ("commission", 0.0003),
        ("stamp_duty", 0.001),
        ("stocklike", True),
        ("commtype", bt.CommInfoBase.COMM_PERC),
    )

    def _getcommission(self, size, price, pseudoexec):
        value = abs(size) * price
        comm = value * self.p.commission
        if size < 0:  # sell
            comm += value * self.p.stamp_duty
        return comm


class RebalanceTopN(bt.Strategy):
    params = dict(topn=5, rank_df=None)

    def __init__(self):
        sig: pd.DataFrame = self.p.rank_df  # type: ignore

        self._by_date: Dict[pd.Timestamp, List[Tuple[str, int]]] = {}
        for d, g in sig.groupby("date"):
            g = g.sort_values(["rank", "code"])
            self._by_date[pd.Timestamp(d)] = list(zip(g["code"].tolist(), g["rank"].astype(int).tolist()))

        self._tradable = {d._name for d in self.datas[1:]}  # skip calendar (data0)

    def next(self):
        dt = pd.Timestamp(self.datas[0].datetime.date(0))
        if dt not in self._by_date:
            return

        items = self._by_date[dt]
        chosen = [c for c, r in items if r <= int(self.p.topn) and c in self._tradable]

        if not chosen:
            for data in self.datas[1:]:
                self.order_target_percent(data=data, target=0.0)
            return

        w = 1.0 / len(chosen)
        chosen_set = set(chosen)

        for data in self.datas[1:]:
            code = data._name
            self.order_target_percent(data=data, target=(w if code in chosen_set else 0.0))


def run_one(
    bars_dir: Path,
    rank_df: pd.DataFrame,
    calendar_code: str | None,
    topn: int,
    cash: float,
    commission: float,
    stamp_duty: float,
    slippage: float,
) -> pd.Series:
    start = rank_df["date"].min()
    end = rank_df["date"].max()

    codes = sorted(rank_df["code"].astype(str).str.zfill(6).unique().tolist())

    # calendar feed
    cal_code = str(calendar_code).zfill(6) if calendar_code else None
    cal_df = pd.DataFrame()
    if cal_code:
        cal_df = load_code_df(bars_dir, cal_code, start=start, end=end)

    if cal_df.empty:
        # fallback to a code from rank_df
        for c in codes:
            cal_df = load_code_df(bars_dir, c, start=start, end=end)
            if not cal_df.empty:
                cal_code = c
                break

    if cal_df.empty:
        raise RuntimeError("Calendar feed not found. Ensure you have local bars under data/bars_qfq.")

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.broker.setcash(cash)
    cerebro.broker.addcommissioninfo(ChinaStockComm(commission=commission, stamp_duty=stamp_duty))
    cerebro.broker.set_slippage_perc(perc=slippage)

    cerebro.adddata(bt.feeds.PandasData(dataname=cal_df), name="__CAL__")

    loaded = 0
    for code in codes:
        df = load_code_df(bars_dir, code, start=start, end=end)
        if df.empty:
            continue
        cerebro.adddata(bt.feeds.PandasData(dataname=df), name=code)
        loaded += 1

    if loaded == 0:
        raise RuntimeError("No tradable feeds loaded. Check your rank codes vs local bars.")

    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="ret", timeframe=bt.TimeFrame.Days)
    cerebro.addstrategy(RebalanceTopN, topn=topn, rank_df=rank_df)

    strat = cerebro.run(maxcpus=1)[0]
    rets = strat.analyzers.ret.get_analysis()

    s = pd.Series(rets, dtype=float)
    s.index = pd.to_datetime(s.index)
    s = s.sort_index().fillna(0.0)

    equity = (1.0 + s).cumprod() * cash
    equity.name = f"Top{topn}"
    return equity


def main():
    root = detect_project_root()

    ap = argparse.ArgumentParser()
    ap.add_argument("--bars-dir", type=str, default=str(root / "data" / "bars_qfq"))
    ap.add_argument("--rank", type=str, default=str(root / "data" / "signals" / "rank_top5.parquet"))
    ap.add_argument("--calendar-code", type=str, default="000001")
    ap.add_argument("--topn-max", type=int, default=5)
    ap.add_argument("--cash", type=float, default=1_000_000)
    ap.add_argument("--commission", type=float, default=0.0003)
    ap.add_argument("--stamp-duty", type=float, default=0.001)
    ap.add_argument("--slippage", type=float, default=0.0005)
    ap.add_argument("--save", type=str, default="")

    # Auto-build rank if missing
    ap.add_argument("--auto-build-rank", type=int, default=1)
    ap.add_argument("--lookback", type=int, default=20)
    ap.add_argument("--rebalance-every", type=int, default=5)
    ap.add_argument("--topk", type=int, default=5)
    ap.add_argument("--min-bars", type=int, default=160)
    ap.add_argument("--no-show", action="store_true", help="不显示图形窗口")

    args = ap.parse_args()

    bars_dir = Path(args.bars_dir)
    rank_path = Path(args.rank)

    print(f"[INFO] ROOT={root}")
    print(f"[INFO] BARS_DIR={bars_dir} exists={bars_dir.exists()}")

    rank_found = locate_rank_file(rank_path, root)

    if rank_found is None:
        if int(args.auto_build_rank) != 1:
            expected = root / "data" / "signals" / "rank_top5.parquet"
            raise FileNotFoundError(
                "Missing rank file.\n"
                f"Tried: {rank_path}\n"
                f"Expected: {expected}\n"
                "You can enable auto build: --auto-build-rank 1\n"
            )

        expected = root / "data" / "signals" / "rank_top5.parquet"
        print(f"[WARN] rank_top5.parquet not found. Auto-building -> {expected}")
        rank_found = build_rank_top5_from_local(
            bars_dir=bars_dir,
            out_path=expected,
            lookback=int(args.lookback),
            rebalance_every=int(args.rebalance_every),
            topk=int(args.topk),
            min_bars=int(args.min_bars),
        )

    print(f"[INFO] Using rank file: {rank_found}")
    rank_df = pd.read_parquet(rank_found).copy()
    rank_df["date"] = pd.to_datetime(rank_df["date"])
    rank_df["code"] = rank_df["code"].astype(str).str.zfill(6)
    rank_df["rank"] = rank_df["rank"].astype(int)

    curves = []
    for n in range(1, int(args.topn_max) + 1):
        print(f"Running Top{n} ...")
        eq = run_one(
            bars_dir=bars_dir,
            rank_df=rank_df,
            calendar_code=(args.calendar_code or "").strip() or None,
            topn=n,
            cash=float(args.cash),
            commission=float(args.commission),
            stamp_duty=float(args.stamp_duty),
            slippage=float(args.slippage),
        )
        curves.append(eq)

    df = pd.concat(curves, axis=1).sort_index().dropna(how="all")
    df_norm = df / df.iloc[0]

    plt.figure(figsize=(12, 6))
    for col in df_norm.columns:
        plt.plot(df_norm.index, df_norm[col].values, label=col)

    plt.title("TopN (1..5) 5-day rebalance equity curves (normalized)")
    plt.xlabel("Date")
    plt.ylabel("Equity (normalized)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    if args.save.strip():
        out = Path(args.save)
        out.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out, dpi=200)
        print(f"Saved plot: {out}")

    if not args.no_show:
        plt.show()


if __name__ == "__main__":
    # 运行回测
    main()