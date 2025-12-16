from pathlib import Path
import pandas as pd
import akshare as ak

OUT = Path("data/meta")
OUT.mkdir(parents=True, exist_ok=True)

def get_all_a_symbols() -> pd.DataFrame:
    # 常用：沪深A股代码名表
    df = ak.stock_info_a_code_name()
    # 列通常是: code, name（不同版本可能列名略有差异）
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={"代码": "code", "名称": "name"}).copy()

    if "code" not in df.columns:
        # 兜底：找包含 code 的列
        code_col = [c for c in df.columns if "code" in c][0]
        df = df.rename(columns={code_col: "code"})

    df["code"] = df["code"].astype(str).str.zfill(6)

    # 过滤：仅保留沪深（0/3/6 开头），剔除北交所（通常 4/8/43/83 等）
    df = df[df["code"].str[0].isin(["0", "3", "6"])].drop_duplicates("code")

    return df[["code"] + (["name"] if "name" in df.columns else [])].reset_index(drop=True)

if __name__ == "__main__":
    df = get_all_a_symbols()
    out_path = OUT / "symbols.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print("Saved:", out_path, "rows=", len(df))
