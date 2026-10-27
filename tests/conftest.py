import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    from common.spark import session
    s = session("smokestream-tests")
    yield s
    s.stop()
