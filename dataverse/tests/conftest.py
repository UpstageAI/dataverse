import os
import sys

import pytest

sys.path.append("./")  # to find etl folder as module


@pytest.fixture(scope="session", autouse=True)
def set_test_mode_env():
    old_value = os.getenv("DATAVERSE_TEST_MODE")

    # Activate test mode
    os.environ["DATAVERSE_TEST_MODE"] = "True"

    # Bring back to previous value
    yield

    if old_value is None:
        del os.environ["DATAVERSE_TEST_MODE"]
    else:
        os.environ["DATAVERSE_TEST_MODE"] = old_value
