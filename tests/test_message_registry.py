"""Реестр подач в разобранном виде (stg_egisz.message_registry) на живой базе.

Сырой слой хранит EGISZ_MESSAGES как в источнике; ключ реестра по MSGID и правило ИЭМК
применяет представление слоя разбора. Строки теста вставляются в транзакции и откатываются.
"""

from __future__ import annotations

import os

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from conftest import load_dag_module  # noqa: E402

connect_pg = load_dag_module("egisz_etl_dag").connect_pg

DSN = os.environ.get("EGISZ_TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="EGISZ_TEST_PG_DSN not set; live-PG tests skipped")

REGISTRY_KEY = "A07167955FA149D1BF532EFAD47EFA46"


def test_message_registry_normalizes_key_and_skips_iemk_document_id() -> None:
    """Ключ реестра — без дефисов, префикса urn:uuid: и угловых скобок, в верхнем регистре.
    Подача на порт ИЭМК localUid документа не несёт."""
    con = connect_pg(DSN)
    try:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_egisz.egisz_messages (egmid, msgid, replyto, documentid, createdate)
                VALUES
                    (-1, 'a0716795-5fa1-49d1-bf53-2efad47efa46', 'http://gost-1.lan:9945', ' UID-OLD ', NULL),
                    (-2, 'urn:uuid:A0716795-5FA1-49D1-BF53-2EFAD47EFA46', 'http://gost-2.lan:9921', 'IEMK-UID', NULL),
                    (-3, '<A07167955FA149D1BF532EFAD47EFA46>', 'http://gost-3.lan:9945', '', NULL),
                    (-4, '  ', NULL, NULL, NULL)
                """
            )
            cur.execute(
                "SELECT egmid, msgid, document_uid, reply_to FROM stg_egisz.message_registry "
                "WHERE egmid < 0 ORDER BY egmid DESC"
            )
            rows = cur.fetchall()
    finally:
        con.rollback()
        con.close()

    assert rows == [
        (-1, REGISTRY_KEY, "uid-old", "http://gost-1.lan:9945"),
        (-2, REGISTRY_KEY, None, "http://gost-2.lan:9921"),
        (-3, REGISTRY_KEY, None, "http://gost-3.lan:9945"),
        (-4, None, None, None),
    ]
