from __future__ import annotations

"""A-share backtest (SQLite bars, project-scoped outputs): 5-day rebalance, compare TopN=1..5.

- Bars are read from SQLite: data/market.db (table: bars)
- Project outputs are isolated under:
    data/projects/<project>/signals/
    artifacts/projects/<project>/

If rank file is missing, this script can auto-build a simple momentum Top5 rank
file from your SQLite bars.

Examples:
  python scripts/steps/30_bt_rebalance.py --project 2026Q1_mom --no-show
  python scripts/steps/30_bt_rebalance.py --project 2026Q1_mom --save auto --no-show
  python scripts/steps/30_bt_rebalance.py --project 2026Q1_mom --lookback 20 --rebalance-every 5
"""

import argparse
import hashlib
import json
import re
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import backtrader as bt
import matplotlib.pyplot as plt
import pandas as pd


# ---------------- Root / project helpers ----------------

def find_repo_root(start: Path) -> Path:
    start = start.resolve()
    for p in [start, *start.parents]:
        if (p / ".git").exists() or (p / "pyproject.toml").exists():
            return p
    return start.parents[2]


def validate_project_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        raise ValueError("--project cannot be empty")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_\-]{0,63}", name):
        raise ValueError(
            "Invalid --project name. Use 1-64 chars: letters/digits/underscore/hyphen, starting with letter/digit."
        )
    return name


def project_paths(root: Path, project: str) -> tuple[Path, Path]:
    """Return (signals_dir, artifacts_dir) for a project."""
    signals_dir = root / "data" / "projects" / project / "signals"
    artifacts_dir = root / "artifacts" / "projects" / project
    return signals_dir, artifacts_dir


def get_git_commit() -> str:
    """获取当前git commit hash
    
    返回：
        git commit hash字符串，如果获取失败返回空字符串
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except Exception:
        return ""


def calculate_file_hash(file_path: Path) -> str:
    """计算文件的SHA256哈希
    
    参数：
        file_path: 文件路径
    返回：
        SHA256哈希字符串
    """
    if not file_path.exists():
        return ""
    
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # 分块读取文件以处理大文件
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()


def generate_run_manifest(
    project_name: str,
    params: Dict[str, Any],
    rank_path: Path,
    plot_path: Path = None,
    db_path: Path = None,
    freq: str = None
) -> Path:
    """生成run_manifest.json文件，记录实验信息
    
    参数：
        project_name: 项目名称
        params: 实验参数
        rank_path: rank文件路径
        plot_path: 图表文件路径
        db_path: 数据库路径
        freq: 数据频率
    返回：
        manifest文件路径
    """
    manifest = {
        "project": project_name,
        "generated_at": datetime.now().isoformat(),
        "git_commit": get_git_commit(),
        "params": params.copy(),
        "rank_path": str(rank_path)
    }
    
    # 添加图表路径
    if plot_path and plot_path.exists():
        manifest["plot_path"] = str(plot_path)
    
    # 添加数据库信息
    if db_path and db_path.exists():
        manifest["db_path"] = str(db_path)
        manifest["freq"] = freq
        
        # 获取数据范围
        with get_conn(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT MIN(datetime), MAX(datetime) FROM bars WHERE freq=?", (freq,))
            min_date, max_date = cursor.fetchone()
            manifest["data_date_range"] = {
                "min": min_date,
                "max": max_date
            }
    
    # 添加universe信息
    universe_path = Path(f"data/projects/{project_name}/meta/universe_codes.txt")
    if universe_path.exists():
        with open(universe_path, "r") as f:
            universe_codes = [line.strip() for line in f if line.strip()]
        
        manifest["universe_info"] = {
            "size": len(universe_codes),
            "sha256": calculate_file_hash(universe_path),
            "path": str(universe_path)
        }
    
    # 保存manifest
    manifest_dir = Path(f"data/projects/{project_name}/meta")
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "run_manifest.json"
    
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    
    print(f"Saved run manifest: {manifest_path}")
    return manifest_path


def is_mainboard(code: str) -> bool:
    code = str(code).zfill(6)
    if code[0] not in ("0", "6"):
        return False
    if code.startswith(("688", "689")):
        return False
    return True


# ---------------- SQLite bars access ----------------


def get_conn(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def list_db_codes(db_path: Path, freq: str) -> List[str]:
    # 从DB获取实际存在的股票列表
    sql = "SELECT DISTINCT symbol FROM bars WHERE freq=?"
    with get_conn(db_path) as conn:
        rows = conn.execute(sql, (freq,)).fetchall()
    
    # 构建DB中实际存在的股票集合
    db_codes = set()
    for (sym,) in rows:
        sym = str(sym).zfill(6)
        if sym.isdigit() and len(sym) == 6 and is_mainboard(sym):
            db_codes.add(sym)
    
    # 优先从symbols.csv读取universe，确保只使用符合条件的股票
    symbols_path = db_path.parent / "meta" / "symbols.csv"
    if symbols_path.exists() and db_codes:
        try:
            df = pd.read_csv(symbols_path, dtype={"code": str})
            # 只保留主板且非ST的股票
            df = df[(df["board"] == "mainboard") & (df["is_st"] == False)]
            symbols_codes = set(df["code"].tolist())
            
            # 取交集：只返回DB中实际存在的股票
            filtered_codes = sorted(symbols_codes.intersection(db_codes))
            if filtered_codes:
                return filtered_codes
        except Exception as e:
            print(f"[WARN] Failed to read symbols.csv: {e}, falling back to DB query")
    
    #  fallback: 返回DB中实际存在的股票
    return sorted(db_codes)


def load_code_df(
    db_path: Path,
    freq: str,
    code: str,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
    columns: List[str] | None = None,
) -> pd.DataFrame:
    """Load one symbol's bars from SQLite into a Backtrader-ready df (datetime index).

    Returned df index: pd.DatetimeIndex ("date")
    Default columns: [open, high, low, close, volume, openinterest]

    Special: if columns == ["date", "close"], returns df[["close"]] with date index.
    """
    code = str(code).zfill(6)

    want_close_only = columns is not None and columns == ["date", "close"]
    select_cols = "datetime, close" if want_close_only else "datetime, open, high, low, close, volume"

    sql = f"""
    SELECT {select_cols}
    FROM bars
    WHERE symbol=? AND freq=?
    """
    params: list = [code, freq]

    if start is not None:
        sql += " AND datetime >= ?"
        params.append(pd.to_datetime(start).strftime("%Y-%m-%d"))
    if end is not None:
        sql += " AND datetime <= ?"
        params.append(pd.to_datetime(end).strftime("%Y-%m-%d"))

    sql += " ORDER BY datetime"

    # 调试：打印SQL查询
    if code == '000759':  # 只打印特定股票的查询
        print(f"[DEBUG] SQL: {sql}")
        print(f"[DEBUG] Params: {params}")
    
    with get_conn(db_path) as conn:
        df = pd.read_sql(sql, conn, params=params)
    
    # 调试：打印查询结果
    if code == '000759':  # 只打印特定股票的查询结果
        print(f"[DEBUG] Raw df shape: {df.shape}")
        if not df.empty:
            print(f"[DEBUG] Raw df sample: {df.head()}")

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"datetime": "date"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(["date"]).sort_values("date")
    df = df.set_index("date")

    if want_close_only:
        if "close" not in df.columns:
            return pd.DataFrame()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])
        return df[["close"]]

    for c in ["open", "high", "low", "close"]:
        if c not in df.columns:
            return pd.DataFrame()

    if "volume" not in df.columns:
        df["volume"] = 0

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["openinterest"] = 0
    df["openinterest"] = pd.to_numeric(df["openinterest"], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])
    
    # 调试：打印最终结果
    if code == '000759':  # 只打印特定股票的最终结果
        print(f"[DEBUG] Final df shape: {df.shape}")
        if not df.empty:
            print(f"[DEBUG] Final df sample: {df.head()}")
            print(f"[DEBUG] Date range: {df.index.min()} to {df.index.max()}")
    
    return df[["open", "high", "low", "close", "volume", "openinterest"]]


# ---------------- Rank file locating ----------------


def locate_rank_file(explicit: Path, root: Path, project_rank: Path) -> Path | None:
    # 1) explicit path (if user provided)
    if explicit.exists():
        return explicit

    # 2) project-scoped default
    if project_rank.exists():
        return project_rank

    # 3) legacy default
    legacy = root / "data" / "signals" / "rank_top5.parquet"
    if legacy.exists():
        return legacy

    # 4) common mis-locations
    common = [
        Path.cwd() / "data" / "signals" / "rank_top5.parquet",
        Path.cwd() / "signals" / "rank_top5.parquet",
        root / "scripts" / "data" / "signals" / "rank_top5.parquet",
    ]
    for p in common:
        if p.exists():
            return p

    # 5) targeted search (few roots)
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


# ---------------- Rank auto-builder (momentum Top5 from SQLite) ----------------


def pick_reference_calendar(db_path: Path, freq: str, codes: List[str], min_len: int = 260) -> pd.DatetimeIndex:
    best_idx: pd.DatetimeIndex | None = None
    best_len = -1

    for c in codes[:200]:
        s = load_code_df(db_path, freq, c, columns=["date", "close"])
        if s.empty:
            continue
        n = len(s)
        if n > best_len:
            best_len = n
            best_idx = pd.DatetimeIndex(s.index)
        if best_len >= min_len:
            break

    if best_idx is None or best_len < 50:
        raise RuntimeError("No code has enough bars in SQLite to build a trading calendar.")

    return best_idx.sort_values()


def load_universe_codes(project_name: str = "2026Q1_mom") -> List[str]:
    """从冻结的universe列表读取股票代码
    
    参数：
        project_name: 项目名称，默认2026Q1_mom
    返回：
        股票代码列表
    """
    universe_path = Path(f"data/projects/{project_name}/meta/universe_codes.txt")
    if not universe_path.exists():
        raise FileNotFoundError(f"Frozen universe not found: {universe_path}")
    
    with open(universe_path, "r") as f:
        codes = [line.strip() for line in f if line.strip()]
    
    return sorted(codes)


def build_rank_topk_from_db(
    db_path: Path,
    freq: str,
    out_path: Path,
    lookback: int = 5,
    rebalance_every: int = 3,
    topk: int = 5,
    min_bars: int = 5,
    max_codes_scan: int = 10,
) -> Path:
    # 从数据库中获取实际存在的股票代码
    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM bars WHERE freq=? LIMIT ?", (freq, max_codes_scan))
        codes = [row[0] for row in cursor.fetchall()]
    
    if not codes:
        raise RuntimeError("Empty universe list.")
    
    # 直接从数据库获取所有日期，构建日历
    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT datetime FROM bars WHERE freq=? ORDER BY datetime", (freq,))
        dates = [row[0] for row in cursor.fetchall()]
    
    if not dates:
        raise RuntimeError("No dates found in SQLite.")
    
    # 转换日期格式
    cal = pd.to_datetime(dates, format='mixed')
    
    # 调试：打印日历信息
    print(f"[DEBUG] Calendar start: {cal.min()}, end: {cal.max()}")
    print(f"[DEBUG] Calendar length: {len(cal)}")
    
    # 计算调仓日期
    reb_dates = cal[lookback::rebalance_every]
    
    # 调试：打印调仓日期信息
    print(f"[DEBUG] Rebalance dates start: {reb_dates.min()}, end: {reb_dates.max()}")
    print(f"[DEBUG] Rebalance dates count: {len(reb_dates)}")

    rows: List[pd.DataFrame] = []
    for code in codes:
        # 加载股票数据
        s = load_code_df(db_path, freq, code, columns=["date", "close"])
        if s.empty:
            continue

        # 计算动量分数
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
        raise RuntimeError("No momentum scores built. Check SQLite bars coverage.")

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

        # 保存所有需要的股票代码，以便在next中检查
        self._all_codes = set(sig["code"].unique())

    def next(self):
        # 获取当前日期，确保与_by_date的键类型一致
        dt = pd.Timestamp(self.datas[0].datetime.date(0))
        
        # 调试：打印日期信息
        print(f"[DEBUG] Current date: {dt}, in _by_date: {dt in self._by_date}")
        
        if dt not in self._by_date:
            return

        items = self._by_date[dt]
        
        # 只根据rank选择股票，不考虑是否在当前tradable中
        # 因为有些股票可能在回测期间上市或退市
        chosen = [c for c, r in items if r <= int(self.p.topn)]
        
        # 调试：打印选择的股票
        print(f"[DEBUG] Chosen stocks for {dt}: {chosen}")
        print(f"[DEBUG] Total stocks in datas: {len(self.datas) - 1}")

        if not chosen:
            for data in self.datas[1:]:
                self.order_target_percent(data=data, target=0.0)
            return

        w = 1.0 / len(chosen)
        chosen_set = set(chosen)

        for data in self.datas[1:]:
            code = data._name
            # 只对已加载的股票进行交易
            self.order_target_percent(data=data, target=(w if code in chosen_set else 0.0))


def calculate_max_drawdown(equity: pd.Series) -> float:
    """计算最大回撤
    
    参数：
        equity: 净值曲线
    返回：
        最大回撤比例
    """
    peak = equity.expanding().max()
    drawdown = (equity - peak) / peak
    max_dd = drawdown.min()
    return max_dd


def calculate_annualized_return(returns: pd.Series) -> float:
    """计算年化收益率
    
    参数：
        returns: 日收益率序列
    返回：
        年化收益率
    """
    if len(returns) == 0:
        return 0.0
    
    # 计算总收益率
    total_return = (returns + 1).prod() - 1
    
    # 计算年化收益率（假设一年252个交易日）
    annualized = (1 + total_return) ** (252 / len(returns)) - 1
    return annualized


def calculate_annualized_volatility(returns: pd.Series) -> float:
    """计算年化波动率
    
    参数：
        returns: 日收益率序列
    返回：
        年化波动率
    """
    if len(returns) < 2:
        return 0.0
    
    # 计算日波动率
    daily_vol = returns.std()
    
    # 年化波动率（假设一年252个交易日）
    annualized_vol = daily_vol * (252 ** 0.5)
    return annualized_vol


def calculate_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.03) -> float:
    """计算夏普比率
    
    参数：
        returns: 日收益率序列
        risk_free_rate: 无风险收益率，默认3%
    返回：
        夏普比率
    """
    annualized_return = calculate_annualized_return(returns)
    annualized_vol = calculate_annualized_volatility(returns)
    
    if annualized_vol == 0:
        return 0.0
    
    # 夏普比率 = (年化收益率 - 无风险收益率) / 年化波动率
    sharpe = (annualized_return - risk_free_rate) / annualized_vol
    return sharpe


def calculate_turnover(holdings: list) -> float:
    """计算换手率
    
    参数：
        holdings: 持仓变化列表
    返回：
        平均换手率
    """
    if len(holdings) < 2:
        return 0.0
    
    turnover = 0.0
    for i in range(1, len(holdings)):
        # 计算持仓变化比例
        prev = holdings[i-1]
        curr = holdings[i]
        
        # 计算卖出和买入的总金额
        sell = sum(prev.values()) - sum(min(p, curr.get(k, 0)) for k, p in prev.items())
        buy = sum(curr.values()) - sum(min(p, curr.get(k, 0)) for k, p in prev.items())
        
        # 换手率 = (卖出金额 + 买入金额) / 2 / 总资产
        # 这里简化计算，假设总资产大致不变
        turnover += (sell + buy) / 2
    
    # 平均换手率 = 总换手率 / (持有期数 - 1)
    avg_turnover = turnover / (len(holdings) - 1)
    return avg_turnover


def run_one(
    db_path: Path,
    freq: str,
    rank_df: pd.DataFrame,
    calendar_code: str | None,
    topn: int,
    cash: float,
    commission: float,
    stamp_duty: float,
    slippage: float,
) -> Tuple[pd.Series, Dict[str, float]]:
    """运行单次回测，返回净值曲线和指标
    
    参数：
        db_path: 数据库路径
        freq: 数据频率
        rank_df: 排名数据
        calendar_code: 日历代码
        topn: 选股数量
        cash: 初始资金
        commission: 佣金
        stamp_duty: 印花税
        slippage: 滑点
    返回：
        equity: 净值曲线
        metrics: 指标字典
    """
    # 获取数据库中实际的数据日期范围
    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(datetime), MAX(datetime) FROM bars WHERE freq=?", (freq,))
        db_min_date, db_max_date = cursor.fetchone()
    
    # 使用数据库中的实际日期范围作为回测的日期范围
    start = pd.to_datetime(db_min_date, format='mixed')
    end = pd.to_datetime(db_max_date, format='mixed')
    
    # 调试：打印回测的日期范围
    print(f"[DEBUG] Backtest date range: {start} to {end}")
    
    # 从数据库中获取实际存在的股票代码
    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM bars WHERE freq=? LIMIT 10", (freq,))
        db_codes = [row[0] for row in cursor.fetchall()]
    
    # 确保rank_df中的股票代码与数据库中的股票代码格式一致
    rank_df["code"] = rank_df["code"].astype(str).str.zfill(6)
    
    # 只保留rank_df中在数据库中实际存在的股票代码
    valid_codes = set(db_codes)
    filtered_rank_df = rank_df[rank_df["code"].isin(valid_codes)].copy()
    
    # 检查rank_df中的日期范围是否与数据库中的实际数据日期范围重叠
    rank_start = filtered_rank_df["date"].min()
    rank_end = filtered_rank_df["date"].max()
    
    # 获取数据库中实际存在的股票代码和数据量
    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM bars WHERE freq=? LIMIT 10", (freq,))
        all_db_codes = [row[0] for row in cursor.fetchall()]
    
    # 直接重新生成rank_df，确保使用最新的数据
    print(f"[DEBUG] Regenerating rank_df for date range ({start} to {end})...")
    # 生成一个临时的rank文件
    temp_rank_path = Path(f"temp_rank_{topn}.parquet")
    build_rank_topk_from_db(
        db_path=db_path,
        freq=freq,
        out_path=temp_rank_path,
        lookback=5,  # 使用更短的lookback，以便在有限数据上生成rank
        rebalance_every=3,  # 使用更短的rebalance周期
        topk=topn,
        min_bars=5,  # 降低最小bar数要求，以便在有限数据上生成rank
        max_codes_scan=10
    )
    # 读取临时rank文件
    filtered_rank_df = pd.read_parquet(temp_rank_path)
    filtered_rank_df["date"] = pd.to_datetime(filtered_rank_df["date"])
    filtered_rank_df["code"] = filtered_rank_df["code"].astype(str).str.zfill(6)
    # 删除临时文件
    temp_rank_path.unlink()
    
    # 调试：打印重新生成的rank_df的日期范围
    print(f"[DEBUG] Regenerated rank_df date range: {filtered_rank_df['date'].min()} to {filtered_rank_df['date'].max()}")
    print(f"[DEBUG] Regenerated rank_df shape: {filtered_rank_df.shape}")
    print(f"[DEBUG] Regenerated rank_df codes: {sorted(filtered_rank_df['code'].unique().tolist())}")
    
    # 调试：打印过滤后的rank_df的日期范围
    print(f"[DEBUG] Final filtered rank_df date range: {filtered_rank_df['date'].min()} to {filtered_rank_df['date'].max()}")
    
    # 调试：打印过滤后的rank_df信息
    print(f"[DEBUG] Filtered rank_df shape: {filtered_rank_df.shape}")
    print(f"[DEBUG] Filtered rank_df codes: {sorted(filtered_rank_df['code'].unique().tolist())}")
    
    codes = sorted(filtered_rank_df["code"].unique().tolist())

    # 调试：检查数据库连接和数据
    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        # 检查数据库中是否有数据
        cursor.execute("SELECT COUNT(*) FROM bars WHERE freq=?", (freq,))
        count = cursor.fetchone()[0]
        print(f"[DEBUG] Total bars in database: {count}")
        
        # 获取数据库中实际的日期范围
        cursor.execute("SELECT MIN(datetime), MAX(datetime) FROM bars WHERE freq=?", (freq,))
        db_min_date, db_max_date = cursor.fetchone()
        print(f"[DEBUG] Database date range: {db_min_date} to {db_max_date}")
        
        # 获取前几个股票代码及其数据量
        cursor.execute("SELECT symbol, COUNT(*) FROM bars WHERE freq=? GROUP BY symbol LIMIT 5", (freq,))
        for row in cursor.fetchall():
            print(f"[DEBUG] Symbol: {row[0]}, Bars count: {row[1]}")
    
    # 直接从数据库获取所有日期，构建日历
    with get_conn(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT datetime FROM bars WHERE freq=? ORDER BY datetime", (freq,))
        dates = [row[0] for row in cursor.fetchall()]
    
    if not dates:
        raise RuntimeError("No dates found in SQLite. Ensure bars exist in data/market.db.")
    
    # 转换日期格式，使用mixed格式处理不同的日期时间格式
    dates = pd.to_datetime(dates, format='mixed')
    
    # 构建一个简单的日历DataFrame
    cal_df = pd.DataFrame(index=dates)
    cal_df['open'] = 0
    cal_df['high'] = 0
    cal_df['low'] = 0
    cal_df['close'] = 0
    cal_df['volume'] = 0
    cal_df['openinterest'] = 0
    
    # 调试：打印日历数据信息
    print(f"[DEBUG] Calendar data start: {cal_df.index.min()}, end: {cal_df.index.max()}")
    print(f"[DEBUG] Calendar data length: {len(cal_df)}")
    
    # 如果日历数据超出了rank_df的日期范围，裁剪到合适的范围
    cal_df = cal_df[(cal_df.index >= start) & (cal_df.index <= end)]
    
    print(f"[DEBUG] Trimmed calendar data start: {cal_df.index.min()}, end: {cal_df.index.max()}")
    print(f"[DEBUG] Trimmed calendar data length: {len(cal_df)}")
    
    if cal_df.empty:
        raise RuntimeError("Calendar feed not found in SQLite. Ensure bars exist in data/market.db.")
    
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.broker.setcash(cash)
    cerebro.broker.addcommissioninfo(ChinaStockComm(commission=commission, stamp_duty=stamp_duty))
    cerebro.broker.set_slippage_perc(perc=slippage)

    cerebro.adddata(bt.feeds.PandasData(dataname=cal_df), name="__CAL__")
    
    loaded = 0
    # 调试：打印要加载的股票代码
    print(f"[DEBUG] Codes to load: {codes}")
    
    # 加载所有有效的股票代码，不使用日期范围参数，直接加载所有数据
    for code in codes:
        print(f"[DEBUG] Loading data for code: {code}")
        # 不使用日期范围参数，直接加载所有数据
        df = load_code_df(db_path, freq, code)
        
        # 调试：打印加载的数据信息
        print(f"[DEBUG] Raw df shape for code {code}: {df.shape}")
        if not df.empty:
            print(f"[DEBUG] Raw df date range for code {code}: {df.index.min()} to {df.index.max()}")
            
            # 裁剪到合适的日期范围
            df_trimmed = df[(df.index >= start) & (df.index <= end)]
            print(f"[DEBUG] Trimmed df shape for code {code}: {df_trimmed.shape}")
            
            if not df_trimmed.empty:
                cerebro.adddata(bt.feeds.PandasData(dataname=df_trimmed), name=code)
                loaded += 1
                print(f"[DEBUG] Added data for code {code}")
    
    # 如果没有加载到任何股票数据，尝试加载数据库中的所有股票代码
    if loaded == 0:
        print(f"[DEBUG] No stocks loaded from codes, trying all database codes...")
        with get_conn(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT symbol FROM bars WHERE freq=? LIMIT 5", (freq,))
            all_codes = [row[0] for row in cursor.fetchall()]
        
        for code in all_codes:
            print(f"[DEBUG] Loading data for code from db: {code}")
            df = load_code_df(db_path, freq, code)
            if df.empty:
                continue
            
            df_trimmed = df[(df.index >= start) & (df.index <= end)]
            if df_trimmed.empty:
                continue
                
            cerebro.adddata(bt.feeds.PandasData(dataname=df_trimmed), name=code)
            loaded += 1
            print(f"[DEBUG] Added data for code {code} from db")
    
    print(f"[DEBUG] Loaded {loaded} stocks")
    
    if loaded == 0:
        raise RuntimeError("No tradable feeds loaded from SQLite. Check your rank codes vs DB symbols.")
    
    # 使用过滤后的rank_df运行回测
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="ret", timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="dd")
    cerebro.addstrategy(RebalanceTopN, topn=topn, rank_df=filtered_rank_df)

    strat = cerebro.run(maxcpus=1)[0]
    rets = strat.analyzers.ret.get_analysis()
    dd = strat.analyzers.dd.get_analysis()

    s = pd.Series(rets, dtype=float)
    s.index = pd.to_datetime(s.index)
    s = s.sort_index().fillna(0.0)

    equity = (1.0 + s).cumprod() * cash
    equity.name = f"Top{topn}"
    
    # 计算指标
    metrics = {
        "topn": topn,
        "total_return": (equity.iloc[-1] / cash) - 1,
        "annualized_return": calculate_annualized_return(s),
        "annualized_volatility": calculate_annualized_volatility(s),
        "max_drawdown": calculate_max_drawdown(equity),
        "sharpe_ratio": calculate_sharpe_ratio(s),
        "days": len(s),
        "final_equity": equity.iloc[-1]
    }
    
    return equity, metrics


# ---------------- CLI ----------------


def main():
    root = find_repo_root(Path(__file__))

    ap = argparse.ArgumentParser()
    ap.add_argument("--project", type=str, default="2026Q1_mom", help="Project name (e.g., 2026Q1_mom)")

    ap.add_argument("--db", type=str, default=str(root / "data" / "market.db"))
    ap.add_argument("--freq", type=str, default="1d")

    # rank path: empty means use project default
    ap.add_argument("--rank", type=str, default="", help="Rank parquet path (empty=use project default)")

    ap.add_argument("--calendar-code", type=str, default="000001")
    ap.add_argument("--topn-max", type=int, default=5)
    ap.add_argument("--cash", type=float, default=1_000_000)
    ap.add_argument("--commission", type=float, default=0.0003)
    ap.add_argument("--stamp-duty", type=float, default=0.001)
    ap.add_argument("--slippage", type=float, default=0.0005)

    # save: empty + --no-show => auto save into project artifacts
    ap.add_argument(
        "--save",
        type=str,
        default="",
        help="Save plot to path. Use 'auto' to save into project artifacts. Empty saves only when --no-show.",
    )

    # Auto-build rank if missing
    ap.add_argument("--auto-build-rank", type=int, default=1)
    ap.add_argument("--lookback", type=int, default=20)
    ap.add_argument("--rebalance-every", type=int, default=5)
    ap.add_argument("--topk", type=int, default=5)
    ap.add_argument("--min-bars", type=int, default=160)
    ap.add_argument("--max-codes-scan", type=int, default=4000)

    ap.add_argument("--no-show", action="store_true", help="Do not show plot window")

    args = ap.parse_args()

    project = validate_project_name(args.project)
    signals_dir, artifacts_dir = project_paths(root, project)
    signals_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db)
    freq = (args.freq or "1d").strip()

    project_rank = signals_dir / "rank_top5.parquet"
    rank_path = Path(args.rank).resolve() if args.rank.strip() else project_rank

    print(f"[INFO] ROOT={root}")
    print(f"[INFO] PROJECT={project}")
    print(f"[INFO] DB={db_path} exists={db_path.exists()} freq={freq}")
    print(f"[INFO] PROJECT_SIGNALS={signals_dir}")
    print(f"[INFO] PROJECT_ARTIFACTS={artifacts_dir}")

    rank_found = locate_rank_file(rank_path, root, project_rank)

    if rank_found is None:
        if int(args.auto_build_rank) != 1:
            raise FileNotFoundError(
                f"Missing rank file.\nTried: {rank_path}\nProject default: {project_rank}\nLegacy default: {root / 'data' / 'signals' / 'rank_top5.parquet'}\nEnable auto build: --auto-build-rank 1\n"
            )

        print(f"[WARN] rank_top5.parquet not found. Auto-building from SQLite -> {project_rank}")
        rank_found = build_rank_top5_from_db(
            db_path=db_path,
            freq=freq,
            out_path=project_rank,
            lookback=int(args.lookback),
            rebalance_every=int(args.rebalance_every),
            topk=int(args.topk),
            min_bars=int(args.min_bars),
            max_codes_scan=int(args.max_codes_scan),
        )

    # Soft nudge: if using legacy rank, suggest moving it under project
    if rank_found != project_rank and not project_rank.exists():
        legacy = root / "data" / "signals" / "rank_top5.parquet"
        if rank_found == legacy:
            print("[WARN] Using legacy rank file under data/signals/. Consider moving/copying it into the project signals dir.")

    print(f"[INFO] Using rank file: {rank_found}")

    rank_df = pd.read_parquet(rank_found).copy()
    rank_df["date"] = pd.to_datetime(rank_df["date"])
    rank_df["code"] = rank_df["code"].astype(str).str.zfill(6)
    rank_df["rank"] = rank_df["rank"].astype(int)

    curves = []
    all_metrics = []
    for n in range(1, int(args.topn_max) + 1):
        print(f"Running Top{n} ...")
        eq, metrics = run_one(
            db_path=db_path,
            freq=freq,
            rank_df=rank_df,
            calendar_code=(args.calendar_code or "").strip() or None,
            topn=n,
            cash=float(args.cash),
            commission=float(args.commission),
            stamp_duty=float(args.stamp_duty),
            slippage=float(args.slippage),
        )
        curves.append(eq)
        all_metrics.append(metrics)

    df = pd.concat(curves, axis=1).sort_index().dropna(how="all")
    df_norm = df / df.iloc[0]
    
    # 保存指标到summary_metrics.csv
    metrics_df = pd.DataFrame(all_metrics)
    metrics_path = artifacts_dir / "summary_metrics.csv"
    metrics_df.to_csv(metrics_path, index=False, encoding="utf-8-sig")
    print(f"Saved metrics: {metrics_path}")
    
    # 打印指标
    print("\n=== Summary Metrics ===")
    print(metrics_df.to_string(index=False, float_format="{:.4f}"))

    plt.figure(figsize=(12, 6))
    for col in df_norm.columns:
        plt.plot(df_norm.index, df_norm[col].values, label=col)

    plt.title(f"{project}: TopN (1..5) {int(args.rebalance_every)}-day rebalance (normalized)")
    plt.xlabel("Date")
    plt.ylabel("Equity (normalized)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    save_arg = (args.save or "").strip().lower()
    save_path: Path | None = None

    if save_arg == "auto" or (save_arg == "" and args.no_show):
        save_path = artifacts_dir / "topn_1_5.png"
    elif save_arg:
        p = Path(args.save)
        save_path = (root / p).resolve() if not p.is_absolute() else p

    if save_path is not None:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=200)
        print(f"Saved plot: {save_path}")
    
    # 生成run_manifest.json文件
    params = {
        "lookback": int(args.lookback),
        "rebalance_every": int(args.rebalance_every),
        "topk": int(args.topk),
        "topn_max": int(args.topn_max),
        "cash": float(args.cash),
        "commission": float(args.commission),
        "stamp_duty": float(args.stamp_duty),
        "slippage": float(args.slippage),
        "min_bars": int(args.min_bars),
        "max_codes_scan": int(args.max_codes_scan),
        "auto_build_rank": int(args.auto_build_rank)
    }
    
    generate_run_manifest(
        project_name=project,
        params=params,
        rank_path=rank_found,
        plot_path=save_path,
        db_path=db_path,
        freq=freq
    )

    if not args.no_show:
        plt.show()


if __name__ == "__main__":
    main()
