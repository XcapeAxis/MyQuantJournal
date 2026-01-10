#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Initialize a quant research / backtesting repo skeleton in the current directory.

Usage:
  python init_repo.py
"""

from __future__ import annotations
from pathlib import Path
import json
import textwrap

ROOT = Path.cwd()

DIRS = [
    "src/quant_mvp",
    "src/quant_mvp/data",
    "src/quant_mvp/features",
    "src/quant_mvp/signals",
    "src/quant_mvp/backtest",
    "src/quant_mvp/metrics",
    "src/quant_mvp/utils",
    "configs",
    "scripts",
    "notebooks",
    "tests",
    "data/raw",
    "data/processed",
    "data/external",
    "data/signals",
    "artifacts/plots",
    "artifacts/reports",
    "docs",
    "logs",
]

FILES = {
    "README.md": textwrap.dedent("""\
        # Quant ML MVP

        A minimal, reproducible pipeline for:
        - data ingestion (incremental bars)
        - feature engineering
        - signal/ranking generation (e.g., top-N)
        - portfolio backtest (rebalancing)
        - evaluation metrics & plots

        ## Quickstart
        1. Create venv and install deps:
           - `python -m venv .venv`
           - Windows: `.venv\\Scripts\\activate`
           - `pip install -r requirements.txt`

        2. Run pipeline (example):
           - `python scripts/update_bars.py --config configs/default.json`
           - `python scripts/generate_rank.py --config configs/default.json`
           - `python scripts/run_backtest.py --config configs/default.json`

        ## Project Structure
        - `src/quant_mvp/` core library
        - `scripts/` CLI entrypoints
        - `configs/` experiment configs (tracked)
        - `data/` local datasets (NOT tracked except placeholders)
        - `artifacts/` outputs (NOT tracked)
        - `docs/` design notes / methodology
        - `tests/` unit tests
        """),
    ".gitignore": textwrap.dedent("""\
        # Python
        __pycache__/
        *.py[cod]
        *.pyo
        *.pyd
        .Python
        .venv/
        venv/
        ENV/
        env/
        .pytest_cache/
        .mypy_cache/
        .ruff_cache/
        .coverage
        htmlcov/
        dist/
        build/
        *.egg-info/

        # Jupyter
        .ipynb_checkpoints/

        # OS / IDE
        .DS_Store
        Thumbs.db
        .idea/
        .vscode/

        # Data & artifacts (keep configs tracked, keep outputs local)
        data/**/*
        !data/**/.gitkeep
        artifacts/**/*
        !artifacts/**/.gitkeep
        logs/**/*
        !logs/**/.gitkeep

        # Secrets
        .env
        *.pem
        *.key
        """),
    "requirements.txt": textwrap.dedent("""\
        # core
        numpy
        pandas
        matplotlib

        # data
        akshare

        # dev
        pytest
        ruff
        """),
    "pyproject.toml": textwrap.dedent("""\
        [project]
        name = "quant-ml-mvp"
        version = "0.1.0"
        description = "Reproducible quant ML + backtesting MVP"
        requires-python = ">=3.10"
        dependencies = []

        [tool.ruff]
        line-length = 100
        target-version = "py310"

        [tool.pytest.ini_options]
        testpaths = ["tests"]
        """),
    "configs/default.json": json.dumps(
        {
            "universe": ["000001.SZ"],
            "start_date": "2018-01-01",
            "end_date": "2025-12-31",
            "bar_freq": "1d",
            "top_n": 5,
            "paths": {
                "data_raw": "data/raw",
                "data_processed": "data/processed",
                "signals": "data/signals",
                "artifacts": "artifacts",
                "logs": "logs"
            }
        },
        ensure_ascii=False,
        indent=2
    ) + "\n",
    "scripts/update_bars.py": textwrap.dedent("""\
        from __future__ import annotations
        import json
        from pathlib import Path

        def main():
            # Placeholder CLI entrypoint.
            # Implement: incremental download/update bars, save to data/raw or data/processed.
            cfg = json.loads(Path("configs/default.json").read_text(encoding="utf-8"))
            print("[update_bars] config loaded:", cfg["bar_freq"], cfg["start_date"], cfg["end_date"])

        if __name__ == "__main__":
            main()
        """),
    "scripts/generate_rank.py": textwrap.dedent("""\
        from __future__ import annotations
        import json
        from pathlib import Path

        def main():
            # Placeholder CLI entrypoint.
            # Implement: read bars/features -> compute scores -> write rank_top{N}.parquet/csv into data/signals/
            cfg = json.loads(Path("configs/default.json").read_text(encoding="utf-8"))
            top_n = cfg.get("top_n", 5)
            out_dir = Path(cfg["paths"]["signals"])
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"rank_top{top_n}.parquet"
            print("[generate_rank] would write:", out_path)

        if __name__ == "__main__":
            main()
        """),
    "scripts/run_backtest.py": textwrap.dedent("""\
        from __future__ import annotations
        import json
        from pathlib import Path

        def main():
            # Placeholder CLI entrypoint.
            # Implement: load rank file -> rebalance -> compute equity curve -> save plots/reports to artifacts/
            cfg = json.loads(Path("configs/default.json").read_text(encoding="utf-8"))
            top_n = cfg.get("top_n", 5)
            rank_path = Path(cfg["paths"]["signals"]) / f"rank_top{top_n}.parquet"
            print("[run_backtest] expecting rank file at:", rank_path)

        if __name__ == "__main__":
            main()
        """),
    "src/quant_mvp/__init__.py": "__all__ = []\n",
    "docs/DECISIONS.md": textwrap.dedent("""\
        # Engineering Decisions

        Record non-trivial decisions here (trade-offs, alternatives considered, rationale).

        ## Log
        - 2026-01-08: Initialized repository skeleton.
        """),
    ".editorconfig": textwrap.dedent("""\
        root = true

        [*]
        charset = utf-8
        end_of_line = lf
        insert_final_newline = true
        indent_style = space
        indent_size = 4

        [*.md]
        trim_trailing_whitespace = false
        """),
    ".pre-commit-config.yaml": textwrap.dedent("""\
        repos:
          - repo: https://github.com/astral-sh/ruff-pre-commit
            rev: v0.5.6
            hooks:
              - id: ruff
              - id: ruff-format
        """),
}

PLACEHOLDERS = [
    "data/raw/.gitkeep",
    "data/processed/.gitkeep",
    "data/external/.gitkeep",
    "data/signals/.gitkeep",
    "artifacts/plots/.gitkeep",
    "artifacts/reports/.gitkeep",
    "logs/.gitkeep",
]

def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        print(f"[skip] exists: {path}")
        return
    path.write_text(content, encoding="utf-8")
    print(f"[ok] wrote: {path}")

def touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        print(f"[skip] exists: {path}")
        return
    path.write_text("", encoding="utf-8")
    print(f"[ok] created: {path}")

def main():
    print(f"Initializing repo skeleton at: {ROOT}")
    for d in DIRS:
        p = ROOT / d
        p.mkdir(parents=True, exist_ok=True)
        print(f"[ok] dir: {p}")

    for rel, content in FILES.items():
        write_file(ROOT / rel, content)

    for rel in PLACEHOLDERS:
        touch(ROOT / rel)

    print("\nDone.")
    print("Next steps:")
    print("  1) git init")
    print("  2) git add . && git commit -m \"chore: init repo skeleton\"")
    print("  3) create GitHub repo and push")

if __name__ == "__main__":
    main()
