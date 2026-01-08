from pathlib import Path
import pandas as pd
import akshare as ak

OUT_DIR = Path("data/meta")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def is_mainboard(code: str) -> bool:
    """沪深主板：
    - 以 0 或 6 开头
    - 排除科创板（688/689）
    - 天然排除创业板（3开头）
    """
    code = str(code).zfill(6)
    if code[0] not in ("0", "6"):
        return False
    if code.startswith(("688", "689")):
        return False
    return True


def build_symbols_csv() -> Path:
    # 兼容不同版本 AKShare 列名
    df = ak.stock_info_a_code_name()
    df.columns = [str(c).lower() for c in df.columns]

    # 常见列名映射
    rename_map = {}
    for c in df.columns:
        if c in ("代码", "code"):
            rename_map[c] = "code"
        if c in ("名称", "name"):
            rename_map[c] = "name"
    df = df.rename(columns=rename_map)

    if "code" not in df.columns:
        # 兜底：找包含 code 的列
        cand = [c for c in df.columns if "code" in c]
        if not cand:
            raise RuntimeError(f"Cannot find code column in {df.columns}")
        df = df.rename(columns={cand[0]: "code"})

    df["code"] = df["code"].astype(str).str.zfill(6)
    df = df[df["code"].apply(is_mainboard)].drop_duplicates("code").reset_index(drop=True)

    out_path = OUT_DIR / "symbols.csv"
    df[["code"] + (["name"] if "name" in df.columns else [])].to_csv(
        out_path, index=False, encoding="utf-8-sig"
    )
    print(f"Saved symbols: {out_path} rows={len(df)}")
    return out_path


if __name__ == "__main__":
    build_symbols_csv()