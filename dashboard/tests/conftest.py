import os
import pytest
from ray.tests.conftest import *  # noqa


@pytest.fixture
def enable_test_module():
    os.environ["RAY_DASHBOARD_MODULE_TEST"] = "true"
    yield
    os.environ.pop("RAY_DASHBOARD_MODULE_TEST", None)


@pytest.fixture
def disable_aiohttp_cache():
    os.environ["RAY_DASHBOARD_NO_CACHE"] = "true"
    yield
    os.environ.pop("RAY_DASHBOARD_NO_CACHE", None)


@pytest.fixture
def set_http_proxy():
    http_proxy = os.environ.get("http_proxy", None)
    https_proxy = os.environ.get("https_proxy", None)

    # set http proxy
    os.environ["http_proxy"] = "www.example.com:990"
    os.environ["https_proxy"] = "www.example.com:990"

    yield

    # reset http proxy
    if http_proxy:
        os.environ["http_proxy"] = http_proxy
    else:
        del os.environ["http_proxy"]

    if https_proxy:
        os.environ["https_proxy"] = https_proxy
    else:
        del os.environ["https_proxy"]


@pytest.fixture
def fast_gcs_failure_detection():
    os.environ["GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR"] = "2"
    os.environ["GCS_CHECK_ALIVE_INTERVAL_SECONDS"] = "1"
    os.environ["GCS_RETRY_CONNECT_INTERVAL_SECONDS"] = "1"
    yield
    os.environ.pop("GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR", None)
    os.environ.pop("GCS_CHECK_ALIVE_INTERVAL_SECONDS", None)
    os.environ.pop("GCS_RETRY_CONNECT_INTERVAL_SECONDS", None)
