"""
Дополняет metabase-field-filters у native-карточек с template-tag type=dimension.

Логика согласована с metabase/setup-dashboards.sh (resolve_field_id_simple):
table_ref + field_name должны совпасть с метаданными Metabase после sync_schema.
"""
from __future__ import annotations

import json
import re
from pathlib import Path


def _resolve(
    tag_name: str,
    tag_def: dict,
    query: str,
) -> tuple[str, str] | None:
    disp = (tag_def or {}).get("display-name") or ""
    q = query or ""

    if tag_name == "parse_date":
        if "v_rpt_network_errors_detail_ui" in q:
            return "public.v_rpt_network_errors_detail_ui", "Дата создания документа"
        if "v_stg_channel_network_errors_by_document" in q:
            return "public.v_stg_channel_network_errors_by_document", "created_at"
        return "public.v_stg_channel_errors_by_document", "created_at"

    if tag_name == "connectivity_day":
        if "v_rpt_clinic_connectivity_daily_ui" in q:
            return "public.v_rpt_clinic_connectivity_daily_ui", "День"
        return "public.v_rpt_connectivity_global_daily_ui", "День"

    if tag_name == "parse_created":
        if "v_rpt_network_errors_detail_ui" in q:
            return "public.v_rpt_network_errors_detail_ui", "Дата создания документа"
        if "v_stg_channel_network_errors_by_document" in q:
            return "public.v_stg_channel_network_errors_by_document", "created_at"
        return "public.v_stg_channel_errors_by_document", "created_at"

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

    # «По дате «Обработано»» и прочие формулировки витрины колбэков
    if "Обработано" in disp and "Отправлено" not in disp:
        return "public.v_egisz_transactions_enriched_ui", "Обработано IPS"

    # Fallback по таблицам в SQL (порядок: узкие витрины раньше)
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
