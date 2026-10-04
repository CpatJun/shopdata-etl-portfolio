"""Project configuration and path helpers."""
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DB = Path(os.getenv("SHOPDATA_SOURCE_DB", PROJECT_ROOT / "shopdata.db"))
TARGET_DB = Path(os.getenv("SHOPDATA_TARGET_DB", PROJECT_ROOT / "analytics.db"))
