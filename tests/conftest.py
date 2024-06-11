import os
import pytest

from revault import Store


PYTEST_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PYTEST_DIR)


@pytest.fixture()
def store(tmpdir):
    path = str(tmpdir.join("test.db"))
    return Store("sqlite:///" + path)
