import pandas as pd
from pathlib import Path

rank_path = Path('d:/我的/quant_mvp/data/projects/2026Q1_mom/signals/rank_top5.parquet')
df = pd.read_parquet(rank_path)
print(f"Date range: {df['date'].min()} to {df['date'].max()}")
print(f"Number of dates: {len(df['date'].unique())}")
print(f"Number of rows: {len(df)}")
print(f"Unique codes: {len(df['code'].unique())}")
