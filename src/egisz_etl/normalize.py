from __future__ import annotations
from typing import Any
import xml.etree.ElementTree as ET

def _extract_xml(text: str | None) -> dict[str, str | None]:
    if not text or "<" not in text: return {}
    try:
        root = ET.fromstring(text.strip().encode("utf-8"))
        tags = {}
        for elem in root.iter():
            tag_name = elem.tag.rsplit("}", 1)[-1].lower()
            if tag_name in ("action", "messageid", "relatesto", "relatestomessage", 
                            "localuid", "emdrid", "documentnumber", "status", 
                            "organization", "message"):
                if elem.text and not tags.get(tag_name):
                    tags[tag_name] = elem.text.strip()
        return tags
    except: return {}

def normalize_exchange_row(row: dict[str, Any]) -> dict[str, Any] | None:
    msg_text = row.get("MSGTEXT")
    log_text = row.get("LOGTEXT")
    
    if row.get("LOGSTATE") == 3:
        return {
            "log_id": row.get("LOGID"),
            "log_date": row.get("LOGDATE"),
            "msg_id": row.get("MSGID"),
            "relates_to": None, "local_uid": None, "emdr_id": None, "doc_num": None,
            "org_oid": None, "status": "error",
            "error_msg": f"Network Error: {log_text}", "callback_url": None
        }

    xml = _extract_xml(msg_text)
    if xml.get("action") == "getDocumentFile": return None
    
    raw_status = xml.get("status", "").lower()
    status = "unknown"
    if "success" in raw_status: status = "success"
    elif "error" in raw_status or (msg_text and "error" in msg_text.lower()): status = "error"

    return {
        "log_id": row.get("LOGID"),
        "log_date": row.get("LOGDATE"),
        "msg_id": xml.get("messageid") or row.get("MSGID"),
        "relates_to": xml.get("relatestomessage") or xml.get("relatesto"),
        "local_uid": xml.get("localuid"),
        "emdr_id": xml.get("emdrid"),
        "doc_num": xml.get("documentnumber"),
        "org_oid": xml.get("organization"),
        "status": status,
        "error_msg": xml.get("message"),
        "callback_url": log_text
    }