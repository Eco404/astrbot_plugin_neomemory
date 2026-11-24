import errno
from types import SimpleNamespace

import httpx
import pytest

from core.provider_errors import is_transient_provider_error


@pytest.mark.parametrize("field,status,retry", [
    ("status_code", 429, True), ("status", 408, True), ("code", 503, True),
    ("code", 400, False), ("status", "UNAVAILABLE", False),
])
def test_numeric_http_status(field, status, retry):
    exc = RuntimeError()
    exc.response = SimpleNamespace(**{field: status})
    assert is_transient_provider_error(exc) is retry


@pytest.mark.parametrize("cause,retry", [
    (httpx.ConnectError("offline"), True), (OSError(errno.ENETUNREACH, "offline"), True),
    (FileNotFoundError(), False), (PermissionError(), False), (ValueError(), False),
])
def test_classifies_transport_cause_without_matching_provider_or_message(cause, retry):
    exc = RuntimeError()
    exc.__cause__ = cause
    assert is_transient_provider_error(exc) is retry
