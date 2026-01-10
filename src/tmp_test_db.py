# tmp_test_db.py
import pandas as pd
from quant_core.data.bars import upsert_bars, load_bars

df = pd.DataFrame(
    {
        "datetime": pd.date_range("2024-01-01", periods=5),
        "open": [1, 2, 3, 4, 5],
        "high": [2, 3, 4, 5, 6],
        "low": [0.5, 1.5, 2.5, 3.5, 4.5],
        "close": [1.5, 2.5, 3.5, 4.5, 5.5],
        "volume": [100, 100, 100, 100, 100],
        "adj_factor": [1, 1, 1, 1, 1],
    }
)

upsert_bars(df, symbol="TEST", freq="1d")

out = load_bars("TEST", "1d")
print(out)
