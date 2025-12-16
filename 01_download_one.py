import os
import pandas as pd
import akshare as ak

DATA_DIR = os.path.join("data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

def download_a_daily(symbol: str, start: str, end: str, adjust: str = "qfq") -> pd.DataFrame:
    # start/end 格式：YYYYMMDD，例如 20180101
    df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start, end_date=end, adjust=adjust)

    # AKShare 返回列名通常为：日期/开盘/收盘/最高/最低/成交量/成交额/振幅/涨跌幅/涨跌额/换手率
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
    df = df.sort_values("date").set_index("date")

    # backtrader 需要 open/high/low/close/volume，openinterest 可补 0
    if "openinterest" not in df.columns:
        df["openinterest"] = 0

    # 转成数值类型，避免字符串导致回测异常
    for c in ["open", "high", "low", "close", "volume", "openinterest"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])
    return df[["open", "high", "low", "close", "volume", "openinterest"]]

if __name__ == "__main__":
    symbol = "600519"  # 贵州茅台示例；你可换成 "000001" 等
    df = download_a_daily(symbol, start="20180101", end="20251231", adjust="qfq")
    out_path = os.path.join(DATA_DIR, f"{symbol}_qfq.parquet")
    df.to_parquet(out_path)
    print(f"Saved: {out_path}, rows={len(df)}")
