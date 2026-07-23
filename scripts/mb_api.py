#!/usr/bin/env python3
"""Единая точка доступа скриптов репозитория к Metabase API: логин и HTTP-хелперы."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

DEFAULT_URL = os.environ.get("MB_URL", "http://localhost:3000")
DEFAULT_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@egisz.local")
DEFAULT_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "egisz")
# Ключ API — для внешних инстансов, где парольная сессия не используется;
# при заданном METABASE_API_KEY сессионный логин не выполняется вовсе.
DEFAULT_API_KEY = os.environ.get("METABASE_API_KEY", "")


def auth_headers(token: str) -> dict[str, str]:
    if DEFAULT_API_KEY:
        return {"x-api-key": DEFAULT_API_KEY}
    return {"X-Metabase-Session": token}


def _urlopen_retry(request: urllib.request.Request, timeout: int, attempts: int = 4, delay: int = 5):
    # Канал до внешнего инстанса нестабилен: обрывы соединения повторяются с паузой;
    # HTTP-ответы (4xx/5xx) не ретраятся — их разбирает вызывающий код.
    for attempt in range(attempts):
        try:
            return urllib.request.urlopen(request, timeout=timeout)
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt == attempts - 1:
                raise
            time.sleep(delay)
    raise AssertionError("unreachable")


def api_json(
    url: str,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    method: str | None = None,
    timeout: int = 30,
) -> object:
    request = urllib.request.Request(
        url,
        data=data,
        headers=headers or {},
        method=method or ("POST" if data else "GET"),
    )
    with _urlopen_retry(request, timeout) as response:
        return json.load(response)


def api(
    method: str,
    path: str,
    token: str,
    payload: dict | None = None,
    base_url: str = DEFAULT_URL,
    timeout: int = 120,
) -> object:
    """Запрос с сессионным токеном или ключом API; HTTP-ошибка разворачивается в RuntimeError с телом ответа."""
    data = None
    headers = auth_headers(token)
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{base_url}{path}", data=data, headers=headers, method=method
    )
    try:
        with _urlopen_retry(request, timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} -> HTTP {exc.code}: {body}") from exc
    return json.loads(body) if body else None


def login(
    base_url: str = DEFAULT_URL,
    email: str = DEFAULT_EMAIL,
    password: str = DEFAULT_PASSWORD,
    timeout: int = 30,
) -> str:
    if DEFAULT_API_KEY:
        return ""
    body = json.dumps({"username": email, "password": password}).encode("utf-8")
    payload = api_json(
        f"{base_url}/api/session",
        data=body,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    session_id = payload.get("id") if isinstance(payload, dict) else None
    if not session_id:
        raise RuntimeError(f"cannot login to Metabase as {email}")
    return session_id
