import time
import pandas as pd
from pathlib import Path
import subprocess

# 记录开始时间
start_time = time.time()

# 运行回测命令，使用500支股票
command = [
    "python", "scripts/steps/11_update_bars.py",
    "--project", "test_performance",
    "--max-codes-scan", "500",
    "--no-show"
]

print(f"Running command: {' '.join(command)}")
result = subprocess.run(command, capture_output=True, text=True)

# 记录结束时间
end_time = time.time()

# 计算耗时
elapsed_time = end_time - start_time

print(f"\n=== Performance Test Results ===")
print(f"Elapsed time: {elapsed_time:.2f} seconds")
print(f"Return code: {result.returncode}")
print(f"\nCommand output (last 10 lines):")
for line in result.stdout.strip().split('\n')[-10:]:
    print(line)
