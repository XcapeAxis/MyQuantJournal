from pathlib import Path
import pandas as pd
import akshare as ak

OUT_DIR = Path("data/meta")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def get_board(code: str) -> str:
    """获取股票所属板块：
    - mainboard: 沪深主板（0/6开头，排除688/689）
    - chinext: 创业板（3开头）
    - star: 科创板（688/689开头）
    - bse: 北交所（8/4开头）
    - unknown: 未知
    """
    code = str(code).zfill(6)
    if code.startswith(("688", "689")):
        return "star"
    elif code.startswith("3"):
        return "chinext"
    elif code.startswith(("8", "4")):
        return "bse"
    elif code.startswith(("0", "6")):
        return "mainboard"
    else:
        return "unknown"


def is_st(name: str) -> bool:
    """判断是否为ST股票：
    - 名称包含 ST 或 *ST
    - 名称包含 退 （退市标记）
    """
    if pd.isna(name):
        return False
    name = str(name).upper()
    return "ST" in name or "退" in name


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

    # 处理股票代码和名称
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].fillna("")
    
    # 添加板块信息
    df["board"] = df["code"].apply(get_board)
    
    # 添加ST标记
    df["is_st"] = df["name"].apply(is_st)
    
    # 过滤规则：
    # 1. 仅保留主板
    # 2. 剔除ST股票
    df = df[(df["board"] == "mainboard") & (df["is_st"] == False)]
    
    # 去重并排序
    df = df.drop_duplicates("code").sort_values("code").reset_index(drop=True)

    out_path = OUT_DIR / "symbols.csv"
    df[["code", "name", "is_st", "board"]].to_csv(
        out_path, index=False, encoding="utf-8-sig"
    )
    print(f"Saved symbols: {out_path} rows={len(df)}")
    return out_path


if __name__ == "__main__":
    build_symbols_csv()