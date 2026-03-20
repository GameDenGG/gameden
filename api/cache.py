import copy
import hashlib
import json
import threading
import time
from collections import defaultdict, deque
from functools import wraps
from inspect import iscoroutinefunction
from typing import Any, Callable

from fastapi import Request
from fastapi.responses import Response
from fastapi.responses import JSONResponse

from api.metrics import record_cache_hit, record_cache_miss


_cache_lock = threading.Lock()
_cache_store: dict[str, tuple[float, Any]] = {}

_rate_lock = threading.Lock()
_rate_store: dict[str, deque[float]] = defaultdict(deque)


def _extract_request(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Request | None:
    for value in kwargs.values():
        if isinstance(value, Request):
            return value
    for value in args:
        if isinstance(value, Request):
            return value
    return None


def _build_cache_key(request: Request) -> str:
    qp_items = sorted(list(request.query_params.multi_items()))
    viewer_scope = (
        request.headers.get("x-gameden-viewer")
        or request.cookies.get("gameden_viewer_id")
        or request.query_params.get("user_id")
        or ""
    )
    return f"{request.url.path}:{qp_items}:viewer={viewer_scope}"


def ttl_cache(ttl_seconds: int, endpoint_key: str | None = None) -> Callable:
    def decorator(func: Callable) -> Callable:
        if iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any):
                request = _extract_request(args, kwargs)
                if request is None or request.method.upper() != "GET":
                    return await func(*args, **kwargs)

                cache_endpoint = endpoint_key or request.url.path
                key = _build_cache_key(request)
                now = time.time()

                with _cache_lock:
                    entry = _cache_store.get(key)
                    if entry and entry[0] > now:
                        record_cache_hit(cache_endpoint)
                        return copy.deepcopy(entry[1])

                record_cache_miss(cache_endpoint)
                result = await func(*args, **kwargs)

                if isinstance(result, Response) and result.status_code >= 400:
                    return result

                with _cache_lock:
                    _cache_store[key] = (now + ttl_seconds, copy.deepcopy(result))

                return result

            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any):
            request = _extract_request(args, kwargs)
            if request is None or request.method.upper() != "GET":
                return func(*args, **kwargs)

            cache_endpoint = endpoint_key or request.url.path
            key = _build_cache_key(request)
            now = time.time()

            with _cache_lock:
                entry = _cache_store.get(key)
                if entry and entry[0] > now:
                    record_cache_hit(cache_endpoint)
                    return copy.deepcopy(entry[1])

            record_cache_miss(cache_endpoint)
            result = func(*args, **kwargs)

            if isinstance(result, Response) and result.status_code >= 400:
                return result

            with _cache_lock:
                _cache_store[key] = (now + ttl_seconds, copy.deepcopy(result))

            return result

        return sync_wrapper

    return decorator


def rate_limit(max_requests: int, window_seconds: int = 60) -> Callable:
    def decorator(func: Callable) -> Callable:
        if iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any):
                request = _extract_request(args, kwargs)
                if request is None:
                    return await func(*args, **kwargs)

                ip = request.client.host if request.client else "unknown"
                key = f"{request.url.path}:{ip}"
                now = time.time()

                with _rate_lock:
                    hits = _rate_store[key]
                    cutoff = now - window_seconds
                    while hits and hits[0] < cutoff:
                        hits.popleft()
                    if len(hits) >= max_requests:
                        return JSONResponse(status_code=429, content={"error": "rate limit exceeded"})
                    hits.append(now)

                return await func(*args, **kwargs)

            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any):
            request = _extract_request(args, kwargs)
            if request is None:
                return func(*args, **kwargs)

            ip = request.client.host if request.client else "unknown"
            key = f"{request.url.path}:{ip}"
            now = time.time()

            with _rate_lock:
                hits = _rate_store[key]
                cutoff = now - window_seconds
                while hits and hits[0] < cutoff:
                    hits.popleft()
                if len(hits) >= max_requests:
                    return JSONResponse(status_code=429, content={"error": "rate limit exceeded"})
                hits.append(now)

            return func(*args, **kwargs)

        return sync_wrapper

    return decorator


def _normalize_etag(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.startswith("W/"):
        cleaned = cleaned[2:].strip()
    if cleaned.startswith('"') and cleaned.endswith('"') and len(cleaned) >= 2:
        cleaned = cleaned[1:-1]
    return cleaned


def json_etag() -> Callable:
    def decorator(func: Callable) -> Callable:
        if iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any):
                request = _extract_request(args, kwargs)
                result = await func(*args, **kwargs)
                return _apply_etag(request, result)

            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any):
            request = _extract_request(args, kwargs)
            result = func(*args, **kwargs)
            return _apply_etag(request, result)

        return sync_wrapper

    return decorator


def _apply_etag(request: Request | None, result: Any) -> Any:
    if request is None or request.method.upper() != "GET":
        return result

    if isinstance(result, Response):
        # Do not rewrite non-JSON or error responses.
        if result.status_code >= 400:
            return result
        content_type = (result.headers.get("content-type") or "").lower()
        if "application/json" not in content_type:
            return result
        return result

    try:
        payload = json.dumps(
            result,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    except Exception:
        return result

    etag_hash = hashlib.sha1(payload).hexdigest()
    etag_value = f"\"{etag_hash}\""
    client_etag = _normalize_etag(request.headers.get("if-none-match"))
    if client_etag == etag_hash:
        return Response(status_code=304, headers={"ETag": etag_value})

    return JSONResponse(content=result, headers={"ETag": etag_value})
