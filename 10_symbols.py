from pathlib import Path
import pandas as pd
import akshare as ak

OUT = Path("data/meta")
OUT.mkdir(parents=True, exist_ok=True)

def get_all_a_symbols() -> pd.DataFrame:
    """获取全A股票列表，包含完整的错误处理和数据验证"""
    try:
        df = ak.stock_info_a_code_name()
        if df is None or df.empty:
            raise ValueError("AKShare returned empty data")
        
        # 列通常是: code, name（不同版本可能列名略有差异）
        df.columns = [c.lower() for c in df.columns]
        df = df.rename(columns={"代码": "code", "名称": "name"}).copy()

        # 查找code列
        if "code" not in df.columns:
            code_candidates = [c for c in df.columns if "code" in c]
            if not code_candidates:
                raise ValueError(f"Cannot find code column in {df.columns.tolist()}")
            code_col = code_candidates[0]
            df = df.rename(columns={code_col: "code"})

        # 验证code列存在
        if "code" not in df.columns:
            raise ValueError("Failed to set code column")

        # 转换代码格式
        df["code"] = df["code"].astype(str).str.zfill(6)
        
        # 验证代码格式（应该是6位数字）
        invalid_codes = df[~df["code"].str.match(r'^\d{6}$')]
        if not invalid_codes.empty:
            print(f"Warning: Found {len(invalid_codes)} invalid codes, filtering them out")
            df = df[df["code"].str.match(r'^\d{6}$')]

        # 过滤：仅保留沪深（0/3/6 开头），剔除北交所（通常 4/8/43/83 等）
        df = df[df["code"].str[0].isin(["0", "3", "6"])].drop_duplicates("code")

        if df.empty:
            raise ValueError("No valid symbols found after filtering")

        result_cols = ["code"]
        if "name" in df.columns:
            result_cols.append("name")
        
        return df[result_cols].reset_index(drop=True)
    except Exception as e:
        print(f"Error in get_all_a_symbols: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        df = get_all_a_symbols()
        if df.empty:
            print("Error: No symbols to save")
            exit(1)
        
        out_path = OUT / "symbols.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print("Saved:", out_path, "rows=", len(df))
    except Exception as e:
        print(f"Fatal error: {e}")
        exit(1)
