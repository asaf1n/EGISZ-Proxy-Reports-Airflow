"""
Дополняет metabase-field-filters у native-карточек и переводит
поддерживаемые text template-tags в настоящие dimension field filters.
"""
from __future__ import annotations

import json
from pathlib import Path


DIMENSION_TAGS = {
    "top_semd",
    "top_clinic",
    "local_uid",
    "relates_to",
    "emdr_id",
    "status",
    "log_id",
    "egmid",
    "err_parse_code",
}


def _resolve(
    tag_name: str,
    tag_def: dict,
    query: str,
) -> tuple[str, str] | None:
    disp = (tag_def or {}).get("display-name") or ""
    q = query or ""

    if tag_name in ("parse_date", "parse_created"):
        if "v_rpt_network_errors_detail_ui" in q:
            return "public.v_rpt_network_errors_detail_ui", "Дата создания документа"
        if "v_stg_channel_network_errors_by_document" in q:
            return "public.v_stg_channel_network_errors_by_document", "created_at"
        return "public.v_stg_channel_errors_by_document", "created_at"

    if tag_name == "connectivity_day":
        if "v_rpt_clinic_connectivity_daily_ui" in q:
            return "public.v_rpt_clinic_connectivity_daily_ui", "День"
        return "public.v_rpt_connectivity_global_daily_ui", "День"

    if tag_name == "top_semd":
        if "v_rpt_documents_no_response_ui" in q and "v_egisz_transactions_enriched_ui" not in q:
            return "public.v_rpt_documents_no_response_ui", "Код СЭМД"
        if "v_rpt_network_errors_detail_ui" in q and "v_egisz_transactions_enriched_ui" not in q:
            return "public.v_rpt_network_errors_detail_ui", "Код СЭМД"
        return "public.v_egisz_transactions_enriched_ui", "Код СЭМД"

    if tag_name == "top_clinic":
        if "v_health_by_clinic_ui" in q:
            return "public.v_health_by_clinic_ui", "JID клиники"
        if "v_rpt_documents_no_response_ui" in q and "v_egisz_transactions_enriched_ui" not in q:
            return "public.v_rpt_documents_no_response_ui", "JID клиники"
        if "v_rpt_network_errors_detail_ui" in q and "v_egisz_transactions_enriched_ui" not in q:
            return "public.v_rpt_network_errors_detail_ui", "JID клиники"
        return "public.v_egisz_transactions_enriched_ui", "JID клиники"

    if tag_name == "local_uid":
        if "v_stg_channel_errors_by_document" in q:
            return "public.v_stg_channel_errors_by_document", "local_uid_hint"
        if "v_rpt_documents_no_response_ui" in q and "v_egisz_transactions_enriched_ui" not in q:
            return "public.v_rpt_documents_no_response_ui", "localUid СЭМД"
        return "public.v_egisz_transactions_enriched_ui", "localUid СЭМД"

    if tag_name == "relates_to":
        if "v_stg_channel_errors_by_document" in q:
            return "public.v_stg_channel_errors_by_document", "relates_to_id"
        return "public.v_egisz_transactions_enriched_ui", "Связанное сообщение"

    if tag_name == "emdr_id":
        if "v_stg_channel_errors_by_document" in q:
            return "public.v_stg_channel_errors_by_document", "emdr_id_hint"
        return "public.v_egisz_transactions_enriched_ui", "Рег. номер РЭМД (emdrid)"

    if tag_name == "status":
        return "public.v_egisz_transactions_enriched_ui", "Статус"

    if tag_name == "log_id":
        if "v_stg_channel_errors_by_document" in q:
            return "public.v_stg_channel_errors_by_document", "exchangelog_log_id"
        return "public.v_egisz_transactions_enriched_ui", "LOGID журнала EXCHANGELOG"

    if tag_name == "egmid":
        if "v_stg_channel_errors_by_document" in q:
            return "public.v_stg_channel_errors_by_document", "egisz_messages_egmid"
        return "public.v_egisz_transactions_enriched_ui", "EGISZ_MESSAGES.EGMID (ключ записи, РЭМД)"

    if tag_name == "err_parse_code":
        return "public.v_stg_channel_errors_by_document", "error_code"

    if tag_name != "dwh_date":
        return None

    if "Отправлено" in disp or "очередь" in disp.lower():
        return "public.v_rpt_documents_no_response_ui", "Отправлено"

    if "День (тренд)" in disp:
        if "v_rpt_semd_archive_ui" in q:
            return "public.v_rpt_semd_archive_ui", "День (тренд)"
        return "public.v_egisz_transactions_enriched_ui", "День (тренд)"

    if "Дата обработки" in disp:
        return "public.v_rpt_semd_archive_ui", "Дата обработки"

    if "Обработано IPS" in disp:
        return "public.v_egisz_transactions_enriched_ui", "Обработано IPS"

    if "Обработано" in disp and "Отправлено" not in disp:
        return "public.v_egisz_transactions_enriched_ui", "Обработано IPS"

    if "v_rpt_documents_no_response_ui" in q and "v_egisz_transactions_enriched_ui" not in q:
        return "public.v_rpt_documents_no_response_ui", "Отправлено"
    if "v_rpt_semd_archive_ui" in q:
        return "public.v_rpt_semd_archive_ui", "Дата обработки"
    if "v_egisz_transactions_enriched_ui" in q:
        return "public.v_egisz_transactions_enriched_ui", "Обработано IPS"

    return None


def patch_file(path: Path) -> int:
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    n = 0
    for card in data.get("cards", []):
        dq = card.get("dataset_query") or {}
        native = dq.get("native") or {}
        query = native.get("query") or ""
        tags = native.get("template-tags") or {}
        if not tags:
            continue

        ff = dict(card.get("metabase-field-filters") or {})
        changed = False
        for tname, tdef in tags.items():
            if tname in DIMENSION_TAGS and (tdef or {}).get("type") != "dimension":
                tdef["type"] = "dimension"
                changed = True
            if (tdef or {}).get("type") != "dimension":
                continue
            if tname in ff:
                continue

            resolved = _resolve(tname, tdef, query)
            if not resolved:
                raise RuntimeError(f"{path.name} / {card.get('name')!r}: cannot resolve dimension {tname!r}")
            tr, fn = resolved
            ff[tname] = {"table_ref": tr, "field_name": fn}
            changed = True
            n += 1

        if changed:
            card["metabase-field-filters"] = ff

    if n:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return n


def main() -> None:
    root = Path(__file__).resolve().parents[1] / "metabase_dashboards"
    total = 0
    for f in sorted(root.glob("*.json")):
        total += patch_file(f)
    print(f"Patched {total} dimension bindings across metabase_dashboards/*.json")


if __name__ == "__main__":
    main()
