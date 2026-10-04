"""Convenience entry point; implementation lives in src/shopdata_etl."""
import logging
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
from shopdata_etl.pipeline import etl_flow  # noqa: E402

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    etl_flow()
