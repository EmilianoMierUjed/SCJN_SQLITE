import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def sqlite_conn():
    conn = sqlite3.connect(":memory:")
    try:
        yield conn
    finally:
        conn.close()
