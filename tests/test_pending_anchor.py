"""Единое правило очереди обработки (README §«Учёт отправленных»).

Статическая часть проверяет, что определение живёт в одном месте — функциях DWH
`is_pending_at` / `pending_segment_code_at`, — а отчётный слой и карточки очереди только
подставляют момент, и что набор документов очереди у всех карточек блока один. Живая часть
требует EGISZ_TEST_PG_DSN (например postgresql://egisz:egisz@localhost:5432/dwh_egisz)
и проверяет свойства, которые статикой не ловятся: неизменность закрытых периодов между
обновлениями витрин, совпадение размера очереди у всех карточек её распределения,
сходимость баланса движения очереди и равенство текущей очереди числу отправленных
без ответа.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from conftest import load_dag_module, load_script_module  # noqa: E402

connect_pg = load_dag_module("egisz_etl_dag").connect_pg

DSN = os.environ.get("EGISZ_TEST_PG_DSN")
live_pg = pytest.mark.skipif(not DSN, reason="EGISZ_TEST_PG_DSN not set; live-PG tests skipped")

ROOT = Path(__file__).resolve().parents[1]
FUNCTIONS_SQL = (ROOT / "db" / "02_functions.sql").read_text(encoding="utf-8")
VIEWS_SQL = (ROOT / "db" / "04_views.sql").read_text(encoding="utf-8")
INTEGRATION_DASHBOARD = ROOT / "metabase_dashboards" / "01_integration_egisz.json"

plan = load_script_module("apply_dashboard_plan")

SURVIVAL = plan.QUEUE_SURVIVAL_NAME
PIVOT_CLINIC = plan.QUEUE_PIVOT_CLINIC_NAME
PIVOT_SEMD = plan.QUEUE_PIVOT_SEMD_NAME
FLOW = plan.QUEUE_FLOW_NAME
TAIL = plan.QUEUE_TAIL_NAME
QUEUE_SIZE = plan.QUEUE_SIZE_NAME
QUEUE_NOW = plan.QUEUE_NOW_NAME
QUEUE_OVER_24H = plan.QUEUE_OVER_24H_NAME
RESCUE = plan.QUEUE_RESCUE_NAME
MAX_AGE = plan.QUEUE_MAX_AGE_NAME

# Карточки, читающие набор документов очереди одним и тем же текстом.
SHARED_CORPUS_CARDS = (PIVOT_CLINIC, PIVOT_SEMD, RESCUE, MAX_AGE, QUEUE_NOW)
QUEUE_CARDS = SHARED_CORPUS_CARDS + (SURVIVAL, FLOW, TAIL, QUEUE_SIZE, QUEUE_OVER_24H)

# Лестница ступеней приходит из справочника; цвета — производные от позиции ступени.
WORKING_SEGMENTS = plan.WORKING_SEGMENTS
SEGMENT_LABELS = [label for _code, label, _minutes, _terminal in WORKING_SEGMENTS]


def dashboard() -> dict:
    return json.loads(INTEGRATION_DASHBOARD.read_text(encoding="utf-8"))


def cards_by_name() -> dict[str, dict]:
    result: dict[str, dict] = {}
    for card in dashboard()["cards"]:
        if card.get("name"):
            result.setdefault(card["name"], card)
    return result


def card_sql(card: dict) -> str:
    return card["dataset_query"]["native"]["query"]


def rendered(sql: str) -> str:
    """SQL без значений фильтров — так его собирает Metabase, когда параметры пусты."""
    return re.sub(r"\[\[.*?\]\]", "", sql)


def shared_corpus() -> str:
    """Общий текст набора документов очереди — общий префикс карточек до закрытия CTE."""
    by_name = cards_by_name()
    prefix = os.path.commonprefix([card_sql(by_name[name]) for name in SHARED_CORPUS_CARDS])
    return prefix[: prefix.rindex(")") + 1]


def function_body(name: str) -> str:
    start = FUNCTIONS_SQL.index(f"CREATE OR REPLACE FUNCTION public.{name}(")
    return FUNCTIONS_SQL[start : FUNCTIONS_SQL.index("\n$$;", start)]


def test_membership_and_segment_have_one_definition() -> None:
    """Возраст, членство и ступень объявлены функциями, а не повторяются в представлениях."""
    membership = function_body("is_pending_at")
    assert "p_first_sent_at <= p_anchor" in membership
    # Границей служит отметка первого ответа, а не длительность до последнего коллбэка.
    assert "p_first_callback_at IS NULL" in membership
    assert "p_first_callback_at > p_anchor" in membership
    assert "p_delivery_seconds" not in membership
    # Предикат — сравнение трёх отметок времени, справочник он не читает.
    assert "IMMUTABLE" in membership
    assert "dim_pending_segments" not in membership
    # Прежняя сигнатура снимается: иначе обе версии сосуществовали бы как перегрузки.
    assert "DROP FUNCTION IF EXISTS public.is_pending_at(timestamptz, numeric, timestamptz);" in FUNCTIONS_SQL

    segment = function_body("pending_segment_code_at")
    # Пороги остаются данными справочника: ужесточение делается UPDATE'ом.
    assert "FROM public.dim_pending_segments s" in segment
    assert "s.max_age_minutes IS NULL" in segment
    assert "ORDER BY s.sort_order" in segment
    assert "STABLE" in segment
    assert not re.search(r"\b\d{3,}\b", segment), "порог ступени захардкожен в функции"

    # Представления только подставляют момент.
    assert "public.pending_segment_code_at(d.first_sent_at, now())" in VIEWS_SQL
    assert "public.pending_segment_code_at(r.first_sent_at, anchor.ts)" in VIEWS_SQL
    assert "<= s.max_age_minutes" not in VIEWS_SQL


def test_first_response_is_persisted_not_derived_from_last_callback() -> None:
    """Выход из очереди опирается на отметку первого ответа, а не на время последнего.

    `last_callback_at` перезаписывается каждым повторным коллбэком (в проде такие
    документы есть), поэтому длительность до него держит отвеченный документ в очереди
    до самого позднего повтора.
    """
    schema_sql = (ROOT / "db" / "01_schema.sql").read_text(encoding="utf-8")
    transform_sql = (ROOT / "db" / "03_transform.sql").read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS first_callback_at timestamptz" in schema_sql
    assert "idx_documents_first_callback_at" in schema_sql
    # Отметка только уменьшается: повторный ответ первого не отменяет.
    assert transform_sql.count("first_callback_at = LEAST(") == 2
    assert "MIN(f.log_date) OVER (PARTITION BY f.dwh_id)" in transform_sql
    assert "d.first_callback_at," in VIEWS_SQL


def test_queue_distribution_lives_in_a_single_card() -> None:
    """Распределение очереди — одна карточка, всегда текущая, на мониторинге."""
    dash = dashboard()
    card = cards_by_name()[QUEUE_NOW]

    assert {c.get("tab") for c in dash["cards"] if c.get("name") == QUEUE_NOW} == {"operational"}
    # Второй карточки того же распределения (на другом моменте) быть не должно.
    assert "Возраст очереди по ступеням" not in {c.get("name") for c in dash["cards"]}
    dimensions = [
        c["visualization_settings"].get("graph.dimensions")
        for c in dash["cards"]
        if c.get("display") == "bar" and c.get("tab") == "sent"
    ]
    assert ["Ступень обработки"] not in dimensions
    assert card["display"] == "bar"


def test_queue_cards_are_pinned_to_the_current_moment() -> None:
    """Все карточки очереди считаются на now() и периодом не режутся."""
    by_name = cards_by_name()

    for name in QUEUE_CARDS:
        sql = card_sql(by_name[name])
        assert "now()" in sql, name
        # Фильтр периода снят: второй даты на вкладке нет.
        assert "period_end" not in sql, name
        # Момент из события постороннего документа: в простое обмена очередь не старела.
        assert "MAX(ips_date)" not in sql, name
        # Левая граница периода отрезала бы отправленное до его начала.
        assert "{{ips_date}}" not in sql, name
        tags = by_name[name]["dataset_query"]["native"]["template-tags"]
        assert "period_end" not in tags, name
        bindings = by_name[name]["metabase-field-filters"]
        assert {"semd_type", "jid"} <= set(bindings), name
        assert bindings["jid"]["table_ref"] == "public.rpt_documents", name
        # Оговорка о фильтре периода объявлена в наименовании карточки.
        assert name.endswith(plan.NO_PERIOD_SUFFIX), name

    dash = dashboard()
    assert not [p for p in dash["parameters"] if p["slug"] in plan.RETIRED_PARAM_IDS]
    retired_ids = set(plan.RETIRED_PARAM_IDS.values())
    for card in dash["cards"]:
        mapped = {m.get("parameter_id") for m in card.get("parameter_mappings") or []}
        assert not mapped & retired_ids, card.get("name")


def test_queue_cards_declare_only_the_tags_they_substitute() -> None:
    """Объявленный тег обязан стоять в SQL: иначе фильтр молча не двигает карточку."""
    for name, card in cards_by_name().items():
        native = card.get("dataset_query", {}).get("native")
        if not native:
            continue
        sql = native["query"]
        for tag in native.get("template-tags", {}):
            assert "{{" + tag + "}}" in sql, f"{name}: тег {tag} объявлен, но не подставлен"
        bindings = card.get("metabase-field-filters") or {}
        assert set(bindings) <= set(native.get("template-tags", {})), name


def test_queue_cards_share_one_document_set() -> None:
    """Набор документов очереди один: членство по первому ответу плюс рабочая ступень."""
    by_name = cards_by_name()
    corpus = shared_corpus()

    assert "public.pending_segment_code_at(" in corpus
    # Справочник ступеней без алиаса: иначе фильтр ступени не развернётся.
    assert "JOIN public.dim_pending_segments ON public.dim_pending_segments.code" in corpus
    assert "public.rpt_documents.first_callback_at" in corpus
    for name in SHARED_CORPUS_CARDS:
        sql = card_sql(by_name[name])
        assert sql.startswith(corpus), name
        # Field filter разворачивается в "public"."rpt_documents".<колонка> — таблица
        # обязана остаться в FROM, и без алиаса.
        assert "FROM public.rpt_documents JOIN public.dim_pending_segments" in sql, name
        assert by_name[name]["metabase-field-filters"]["pending_segment"] == {
            "table_ref": "public.dim_pending_segments",
            "field_name": "label",
        }, name

    # Последняя ступень («ответа уже не ждут») исключена из очереди во всех карточках
    # блока: расхождение этого правила и давало несходящиеся итоги.
    for name in QUEUE_CARDS:
        assert "is_no_response" in card_sql(by_name[name]), name


def test_age_distribution_is_not_a_funnel() -> None:
    """Ступени возраста — взаимоисключающая гистограмма, а не воронка конверсии."""
    by_name = cards_by_name()
    dash = dashboard()

    assert "В обработке на конец периода" not in by_name
    histogram = by_name[QUEUE_NOW]
    viz = histogram["visualization_settings"]
    assert viz["graph.dimensions"] == ["Ступень обработки"]
    assert viz["graph.x_axis.scale"] == "ordinal"
    # Подписи рядов — ровно рабочие ступени справочника: последняя сюда не попадает.
    assert list(viz["series_settings"]) == SEGMENT_LABELS
    sql = card_sql(histogram)
    assert "GROUP BY segment_label, segment_sort" in sql
    assert "age_minutes >" not in sql

    survival = by_name[SURVIVAL]
    assert survival["display"] == "line"
    survival_sql = card_sql(survival)
    assert "FILTER (WHERE q.age_minutes > g.max_age_minutes)" in survival_sql
    # Подпись оси называет то, что под ней стоит: ряд считает документы СТАРШЕ границы.
    assert "regexp_replace(g.label, '^до ', 'дольше ')" in survival_sql
    assert survival["visualization_settings"]["graph.dimensions"] == ["Срок ожидания", "Срез"]
    assert set(survival["visualization_settings"]["series_settings"]) == {
        plan.QUEUE_SURVIVAL_SLICE_NOW,
        plan.QUEUE_SURVIVAL_SLICE_WEEK_AGO,
    }

    # Воронка осталась только у скорости регистрации — там шаги действительно про срок.
    funnels = {c["name"] for c in dash["cards"] if c.get("display") == "funnel"}
    assert funnels == {"Скорость регистрации в РЭМД"}


def test_rescue_card_has_no_zero_goal_progress_bar() -> None:
    """Цель карточки — ноль, а полоса прогресса делит на цель: подача сведена к счётчику."""
    card = cards_by_name()[RESCUE]
    assert card["display"] == "scalar"
    assert "progress.goal" not in card["visualization_settings"]


def test_queue_flow_separates_its_two_outflows() -> None:
    """У водопада один цвет убыли, поэтому он нейтрален, а шаги различаются подписью."""
    card = cards_by_name()[FLOW]
    viz = card["visualization_settings"]
    assert viz["waterfall.decrease_color"] == plan.SENT_STATE_COLORS["no_response"]
    assert viz["waterfall.decrease_color"] != "#84BB4C", "зелёный подписывал утилизацию успехом"
    assert viz["waterfall.total_color"] != viz["waterfall.increase_color"]
    assert card["description"].count("подписью") == 1


def test_queue_matrices_are_heatmaps_with_model_drill() -> None:
    """«Объект × ступень» — матрица с условным форматированием и дриллом в модель.

    Сводная таблица Metabase доступна только вопросам конструктора запросов
    («Сводные таблицы поддерживаются только для вопросов, созданных в конструкторе»),
    поэтому разворот делает сам запрос, а тепловую карту — форматирование колонок.
    """
    by_name = cards_by_name()
    for name, dimension in ((PIVOT_CLINIC, "Клиника"), (PIVOT_SEMD, "Код СЭМД")):
        card = by_name[name]
        assert card["display"] == "table", name
        viz = card["visualization_settings"]
        columns = [column["name"] for column in viz["table.columns"]]
        assert columns == [dimension, *SEGMENT_LABELS, "Всего"], name
        formatting = viz["table.column_formatting"]
        assert formatting[0]["type"] == "range", name
        assert formatting[0]["columns"] == SEGMENT_LABELS, name
        sql = card_sql(card)
        # Отбор — по коду ступени: переименование в справочнике меняет заголовок колонки
        # и не обнуляет её значения.
        for code, label, _minutes, _terminal in WORKING_SEGMENTS:
            assert f"FILTER (WHERE segment_code = '{code}')" in sql, (name, code)
            assert f'AS "{label}"' in sql, (name, label)
        assert "segment_label = '" not in sql, name
        click = card["click_behavior"]
        assert click["linkType"] == "question", name
        sources = {
            (spec.get("source") or {}).get("name") for spec in click["parameterMapping"].values()
        }
        assert sources == {dimension}, name


@pytest.fixture
def con():
    con = connect_pg(DSN)
    yield con
    con.rollback()
    con.close()


@live_pg
def test_refresh_report_marts_keeps_closed_periods_unchanged(con) -> None:
    """Двойной refresh_report_marts() не двигает строки закрытых недель и месяцев."""
    marts = (
        ("public.rpt_documents_weekly", "is_complete_week"),
        ("public.rpt_documents_monthly", "is_complete_month"),
    )
    snapshots: dict[str, list[tuple]] = {}
    with con.cursor() as cur:
        cur.execute("SELECT public.refresh_report_marts()")
        con.commit()
        for mart, flag in marts:
            cur.execute(f"SELECT * FROM {mart} WHERE {flag} ORDER BY 1, 2, 4")
            snapshots[mart] = cur.fetchall()

        cur.execute("SELECT public.refresh_report_marts()")
        con.commit()
        for mart, flag in marts:
            cur.execute(f"SELECT * FROM {mart} WHERE {flag} ORDER BY 1, 2, 4")
            assert cur.fetchall() == snapshots[mart], mart


@live_pg
def test_queue_cards_agree_on_the_queue_size(con) -> None:
    """Гистограмма, сводные и кривая долей описывают одну и ту же очередь."""
    by_name = cards_by_name()
    sizes: dict[str, int] = {}
    with con.cursor() as cur:
        # Один транзакционный now(): момент не должен разъезжаться между карточками
        # во время самой проверки.
        for name in (PIVOT_CLINIC, PIVOT_SEMD):
            cur.execute(rendered(card_sql(by_name[name])))
            sizes[name] = sum(row[-1] for row in cur.fetchall())

        cur.execute(rendered(card_sql(by_name[QUEUE_NOW])))
        sizes[QUEUE_NOW] = sum(row[-1] for row in cur.fetchall())

        survival_corpus = rendered(card_sql(by_name[SURVIVAL])).split(", totals AS")[0]
        cur.execute(
            f"{survival_corpus} SELECT COUNT(DISTINCT dwh_id) FROM queue "
            f"WHERE slice = '{plan.QUEUE_SURVIVAL_SLICE_NOW}'"
        )
        sizes[SURVIVAL] = cur.fetchone()[0]

        cur.execute(f"{rendered(shared_corpus())} SELECT COUNT(DISTINCT dwh_id) FROM queue")
        sizes["набор документов"] = cur.fetchone()[0]
    con.rollback()

    assert len(set(sizes.values())) == 1, sizes


@live_pg
def test_current_queue_equals_documents_awaiting_a_response(con) -> None:
    """Текущая очередь — ровно отправленные без ответа: status = 'sent' на рабочей ступени."""
    with con.cursor() as cur:
        cur.execute(f"{rendered(shared_corpus())} SELECT COUNT(DISTINCT dwh_id) FROM queue")
        queue_size = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM public.rpt_documents d "
            "JOIN public.dim_pending_segments g "
            "ON g.code = public.pending_segment_code_at(d.first_sent_at, now()) "
            "WHERE d.status = 'sent' AND NOT g.is_no_response"
        )
        awaiting = cur.fetchone()[0]
        # Отметка первого ответа и статус документа обязаны говорить об одном и том же.
        cur.execute(
            "SELECT COUNT(*) FROM public.rpt_documents "
            "WHERE (status = 'sent') <> public.is_pending_at(first_sent_at, first_callback_at, now()) "
            "AND first_sent_at IS NOT NULL"
        )
        disagreements = cur.fetchone()[0]
    con.rollback()

    assert queue_size == awaiting
    assert disagreements == 0


@live_pg
def test_queue_flow_balances_to_the_queue(con) -> None:
    """Движение очереди сходится: начало + поступило − ответы − утилизация = конец."""
    by_name = cards_by_name()
    with con.cursor() as cur:
        cur.execute(rendered(card_sql(by_name[FLOW])))
        moves = {row[1]: row[2] for row in cur.fetchall()}
        cur.execute(
            "SELECT COUNT(*) FROM public.rpt_documents d "
            "JOIN public.dim_pending_segments g "
            "ON g.code = public.pending_segment_code_at(d.first_sent_at, now()) "
            "WHERE public.is_pending_at(d.first_sent_at, d.first_callback_at, now()) "
            "AND NOT g.is_no_response"
        )
        queue = cur.fetchone()[0]
    con.rollback()

    assert sum(moves.values()) == queue, moves
