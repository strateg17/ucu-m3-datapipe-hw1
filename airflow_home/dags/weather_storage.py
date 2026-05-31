from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def ensure_parent_dir(file_path: str) -> None:
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)


def write_json(file_path: str, payload: Any) -> str:
    ensure_parent_dir(file_path)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return file_path


def read_json(file_path: str) -> Any:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def file_exists(file_path: str) -> bool:
    return Path(file_path).exists()
