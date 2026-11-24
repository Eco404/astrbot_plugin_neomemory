"""Transport and HTTP failures shared by model and embedding requests."""

import errno
import socket

from aiohttp import ClientConnectionError
from httpx import NetworkError, TimeoutException, RemoteProtocolError

_NETWORK_ERRNOS = {
    errno.ECONNRESET, errno.ECONNREFUSED, errno.ECONNABORTED, errno.ENETUNREACH,
    errno.EHOSTUNREACH, errno.ENETDOWN, errno.ETIMEDOUT, errno.EPIPE,
}


def is_transient_provider_error(exc: BaseException) -> bool:
    """Use transport exceptions and numeric HTTP status, including wrapped causes."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, (TimeoutError, ConnectionError, socket.gaierror, ClientConnectionError,
                            NetworkError, TimeoutException, RemoteProtocolError)):
            return True
        if isinstance(exc, OSError) and exc.errno in _NETWORK_ERRNOS:
            return True
        for source in (exc, getattr(exc, "response", None)):
            for key in ("status_code", "status", "code"):
                status = getattr(source, key, None)
                if isinstance(status, int) and 100 <= status < 600:
                    return status in (408, 429) or status >= 500
        last_attempt = getattr(exc, "last_attempt", None)
        exc = (last_attempt.exception() if getattr(last_attempt, "failed", False)
               else exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__))
    return False
