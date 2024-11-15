from pathlib import Path

import pytest

TEST_DIR = Path(__file__).parent


def pytest_addoption(parser):
    parser.addoption("--update-results", action="store_true", default=False)
    parser.addoption("--output-dir", action="store", default="test_results")


@pytest.fixture(scope="session")
def update_results(pytestconfig) -> bool:
    return pytestconfig.getoption("update_results")


@pytest.fixture(scope="session")
def output_dir(pytestconfig) -> Path:
    return TEST_DIR / pytestconfig.getoption("output_dir")
