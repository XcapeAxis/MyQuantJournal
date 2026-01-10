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


def safe_parse_boolean(value) -> bool:
    """安全解析布尔值，支持多种格式：
    - 布尔值：True/False
    - 字符串："true"/"false"/"TRUE"/"FALSE"/"1"/"0"
    - 数字：1/0
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        value = value.strip().lower()
        if value in ("true", "t", "1", "yes", "y"):
            return True
        if value in ("false", "f", "0", "no", "n"):
            return False
    return False


def filter_universe(symbols_df: pd.DataFrame) -> pd.DataFrame:
    """Universe过滤函数（可复用）
    过滤规则：
    1. board == "mainboard"
    2. is_st == False
    3. 排除300/301/688/689/8xx/4xx前缀（以防symbols.csv出错）
    
    参数：
        symbols_df: 包含code, is_st, board列的DataFrame
    返回：
        过滤后的DataFrame
    """
    df = symbols_df.copy()
    
    # 安全解析is_st列
    df["is_st"] = df["is_st"].apply(safe_parse_boolean)
    
    # 1. 基本过滤：主板且非ST
    df = df[(df["board"] == "mainboard") & (df["is_st"] == False)]
    
    # 2. 额外保险：排除特定前缀
    def is_valid_code(code):
        code = str(code).zfill(6)
        invalid_prefixes = ["300", "301", "688", "689", "8", "4"]
        return not any(code.startswith(prefix) for prefix in invalid_prefixes)
    
    df = df[df["code"].apply(is_valid_code)]
    
    # 去重并排序
    df = df.drop_duplicates("code").sort_values("code").reset_index(drop=True)
    
    return df


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
    
    # 使用统一过滤函数
    df = filter_universe(df)
    
    # 保存完整symbols.csv（处理可能的权限问题）
    out_path = OUT_DIR / "symbols.csv"
    try:
        df[["code", "name", "is_st", "board"]].to_csv(
            out_path, index=False, encoding="utf-8-sig"
        )
        print(f"Saved symbols: {out_path} rows={len(df)}")
    except Exception as e:
        print(f"Warning: Failed to save symbols.csv: {e}")
        print("Continuing to generate frozen universe...")
    
    # 生成冻结的universe列表（用于2026Q1_mom项目）
    project_meta_dir = Path("data/projects/2026Q1_mom/meta")
    project_meta_dir.mkdir(parents=True, exist_ok=True)
    
    universe_path = project_meta_dir / "universe_codes.txt"
    try:
        with open(universe_path, "w") as f:
            for code in df["code"]:
                f.write(f"{code}\n")
        
        print(f"Saved frozen universe: {universe_path} rows={len(df)}")
        print(f"Universe size: {len(df)}")
        print(f"Sample codes: {', '.join(df['code'].head(10).tolist())}")
    except Exception as e:
        print(f"Error: Failed to save frozen universe: {e}")
        raise
    
    return out_path


if __name__ == "__main__":
    build_symbols_csv()