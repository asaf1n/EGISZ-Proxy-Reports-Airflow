from __future__ import annotations

from typing import Any
import re
import xml.etree.ElementTree as ET


_UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
_OID_RE = re.compile(r"\b(?:\d+\.){3,}\d+\b")
_HOST_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b")
_LONG_NUMBER_RE = re.compile(r"\b\d{6,}\b")
_TAG_RE = re.compile(r"<[^>]+>")


def _first_text_by_local_name(root: ET.Element, local_names: tuple[str, ...]) -> str | None:
    wanted = {name.lower() for name in local_names}
    for elem in root.iter():
        local = elem.tag.rsplit("}", 1)[-1].lower()
        if local in wanted and elem.text and elem.text.strip():
            return elem.text.strip()
    return None


def _parse_xml_fields(text: str | None) -> dict[str, str | None]:
    if not text or "<" not in text:
        return {}
    try:
        root = ET.fromstring(text.encode("utf-8"))
    except Exception:
        try:
            root = ET.fromstring(text)
        except Exception:
            return {}
    return {
        "message_id": _first_text_by_local_name(root, ("MessageID",)),
        "relates_to": _first_text_by_local_name(root, ("RelatesTo", "relatesToMessage")),
        "local_uid": _first_text_by_local_name(root, ("localUid",)),
        "organization_oid": _first_text_by_local_name(root, ("organization",)),
        "document_kind": _first_text_by_local_name(root, ("kind",)),
    }


def clean_error_text(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = _TAG_RE.sub(" ", text)
    cleaned = _HOST_RE.sub("[url]", cleaned)
    cleaned = _IP_RE.sub("[ip]", cleaned)
    cleaned = _UUID_RE.sub("[uuid]", cleaned)
    cleaned = _OID_RE.sub("[oid]", cleaned)
    cleaned = _LONG_NUMBER_RE.sub("[num]", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:1000] if cleaned else None


def status_category(row: dict[str, Any], message_text: str | None) -> str:
    state = row.get("LOGSTATE")
    try:
        if int(state) == 3:
            return "success"
    except Exception:
        pass
    text = (message_text or "").lower()
    if any(marker in text for marker in ("fault", "exception", "error", "ошиб", "failed")):
        return "error"
    if state is not None:
        return "unknown"
    return "unknown"


def normalize_exchange_row(row: dict[str, Any]) -> dict[str, Any]:
    msg_text = row.get("MSGTEXT")
    log_text = row.get("LOGTEXT")
    xml_fields = _parse_xml_fields(msg_text)
    status = status_category(row, msg_text)
    error_text = msg_text if status == "error" else None
    return {
        "log_id": row.get("LOGID"),
        "log_date": row.get("LOGDATE"),
        "log_type": row.get("LOGTYPE"),
        "log_state": row.get("LOGSTATE"),
        "log_mode": row.get("LOGMODE"),
        "msg_id": row.get("MSGID"),
        "group_id": row.get("GRPID"),
        "created_at": row.get("CREATEDATE"),
        "modified_at": row.get("MODIFYDATE"),
        "repl_id": row.get("REPL$ID"),
        "repl_group_id": row.get("REPL$GRPID"),
        "endpoint": log_text,
        "method": row.get("METHOD"),
        "uri": row.get("URI"),
        "action": row.get("ACTION") or xml_fields.get("action"),
        "parent_log_id": row.get("PARENTLOGID"),
        "message_id": xml_fields.get("message_id"),
        "relates_to": xml_fields.get("relates_to"),
        "local_uid": xml_fields.get("local_uid"),
        "organization_oid": xml_fields.get("organization_oid"),
        "document_kind": xml_fields.get("document_kind"),
        "status_category": status,
        "error_text": error_text,
        "clean_error_text": clean_error_text(error_text),
    }
