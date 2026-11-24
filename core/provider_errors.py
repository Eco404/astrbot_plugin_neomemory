"""Transport and HTTP failures shared by model and embedding requests."""

import errno
import socket

from aiohttp import ClientConnectionError
from httpx import NetworkError, TimeoutException, RemoteProtocolError

_NETWORK_ERRNOS = {
    errno.ECONNRESET, errno.ECONNREFUSED, errno.ECONNABORTED, errno.ENETUNREACH,
    errno.EHOSTUNREACH, errno.ENETDOWN, errno.ETIMEDOUT, errno.EPIPE,
}


def exception_chain(exc):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        last_attempt = getattr(exc, "last_attempt", None)
        exc = (last_attempt.exception() if getattr(last_attempt, "failed", False)
               else exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__))


def http_status(exc):
    for cause in exception_chain(exc):
        for source in (cause, getattr(cause, "response", None)):
            for key in ("status_code", "status", "code"):
                status = getattr(source, key, None)
                if isinstance(status, int) and 100 <= status < 600:
                    return int(status)
    return None


def is_transient_provider_error(exc: BaseException) -> bool:
    status = http_status(exc)
    if status is not None:
        return status in (408, 429) or status >= 500
    return any(isinstance(cause, (TimeoutError, ConnectionError, socket.gaierror, ClientConnectionError,
                                  NetworkError, TimeoutException, RemoteProtocolError))
               or isinstance(cause, OSError) and cause.errno in _NETWORK_ERRNOS
               for cause in exception_chain(exc))
