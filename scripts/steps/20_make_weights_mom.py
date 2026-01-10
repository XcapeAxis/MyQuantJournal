from pathlib import Path
import pandas as pd

BARS_DIR = Path("data/bars_qfq")
OUT = Path("data/signals")
OUT.mkdir(parents=True, exist_ok=True)

def load_code_df(code: str) -> pd.DataFrame:
    code_dir = BARS_DIR / f"code={code}"
    parts = list(code_dir.rglob("*.parquet"))
    if not parts:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")

def make_weights(codes: list[str], topn=50, lookback=20, rebalance_every=5):
    # 先做一个“能跑通”的版本：收集各股票 close 序列
    frames = []
    for c in codes:
        df = load_code_df(c)
        if df.empty:
            continue
        frames.append(df[["date", "close"]].assign(code=c))
    panel = pd.concat(frames, ignore_index=True)
    panel = panel.sort_values(["code", "date"])

    panel["ret_lb"] = panel.groupby("code")["close"].pct_change(lookback)
    # 选出每个交易日的横截面分数
    panel = panel.dropna(subset=["ret_lb"])

    # 生成调仓日序列（按“出现的交易日”每 5 根bar调仓一次）
    all_dates = panel["date"].drop_duplicates().sort_values().reset_index(drop=True)
    reb_dates = all_dates.iloc[::rebalance_every].tolist()

    out_rows = []
    for d in reb_dates:
        snap = panel[panel["date"] == d].sort_values("ret_lb", ascending=False).head(topn)
        if snap.empty:
            continue
        w = 1.0 / len(snap)
        for _, r in snap.iterrows():
            out_rows.append((d, r["code"], w))

    out = pd.DataFrame(out_rows, columns=["date", "code", "weight"])
    out.to_parquet(OUT / "target_weights.parquet", index=False)
    print("Saved:", OUT / "target_weights.parquet", "rows=", len(out))

if __name__ == "__main__":
    # 先用少量股票验证框架（强烈建议），跑通后再扩大
    # 例如读 data/meta/symbols.csv 取前 300 只
    symbols = pd.read_csv("data/meta/symbols.csv", dtype={"code": str})["code"].head(300).tolist()
    make_weights(symbols, topn=50, lookback=20, rebalance_every=5)
