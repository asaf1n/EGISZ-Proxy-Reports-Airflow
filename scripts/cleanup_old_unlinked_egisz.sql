\set ON_ERROR_STOP on

\if :{?cleanup_before}
\else
\set cleanup_before '2026-07-20 00:00:00 Europe/Moscow'
\endif

\if :{?apply_cleanup}
\else
\set apply_cleanup false
\endif

\echo cleanup_before = :cleanup_before
\echo apply_cleanup = :apply_cleanup

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30min';

CREATE TEMP TABLE cleanup_documents ON COMMIT DROP AS
SELECT d.dwh_id
FROM mart_egisz.documents d
WHERE d.status = mart_egisz.document_status_nonfinal()
  AND d.first_sent_at < :'cleanup_before'::timestamptz
  AND NOT EXISTS (
      SELECT 1
      FROM stg_egisz.exchange_messages t
      WHERE t.dwh_id = d.dwh_id
        AND t.status IN ('success', 'error')
  );

CREATE TEMP TABLE cleanup_unlinked_logids ON COMMIT DROP AS
SELECT DISTINCT tx.logid
FROM stg_egisz.exchange_messages tx
WHERE tx.log_date < :'cleanup_before'::timestamptz
  AND tx.dwh_id IS NULL
  AND (
      tx.link_method IN ('unlinked', 'message_registry_no_document')
      OR (
          tx.source_action = 'getDocumentFile'
          AND tx.xml_dwh_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM stg_egisz.message_registry m
              WHERE m.document_uid = tx.xml_dwh_id
          )
      )
  );

CREATE TEMP TABLE cleanup_logids ON COMMIT DROP AS
SELECT DISTINCT logid
FROM (
    SELECT t.logid
    FROM stg_egisz.exchange_messages t
    JOIN cleanup_documents d
      ON t.dwh_id = d.dwh_id
      OR t.xml_dwh_id = d.dwh_id

    UNION ALL
    SELECT request_logid
    FROM mart_egisz.documents d
    JOIN cleanup_documents c ON c.dwh_id = d.dwh_id
    WHERE request_logid IS NOT NULL

    UNION ALL
    SELECT result_logid
    FROM mart_egisz.documents d
    JOIN cleanup_documents c ON c.dwh_id = d.dwh_id
    WHERE result_logid IS NOT NULL

    UNION ALL
    SELECT logid
    FROM cleanup_unlinked_logids
) s
WHERE logid IS NOT NULL;

CREATE TEMP TABLE cleanup_registry_keys ON COMMIT DROP AS
SELECT DISTINCT stg_egisz.message_registry_key(t.relates_to_msgid) AS msgid
FROM stg_egisz.exchange_messages t
JOIN cleanup_logids l ON l.logid = t.logid
WHERE t.relates_to_msgid IS NOT NULL
  AND stg_egisz.message_registry_key(t.relates_to_msgid) IS NOT NULL;

CREATE INDEX ON cleanup_registry_keys (msgid);

CREATE TEMP TABLE cleanup_registry_rows ON COMMIT DROP AS
SELECT DISTINCT egmid, msgid, document_uid
FROM (
    SELECT m.egmid, m.msgid, m.document_uid
    FROM stg_egisz.message_registry m
    JOIN cleanup_documents d ON d.dwh_id = m.document_uid
    WHERE COALESCE(m.created_at, '-infinity'::timestamptz) < :'cleanup_before'::timestamptz

    UNION ALL
    SELECT m.egmid, m.msgid, m.document_uid
    FROM stg_egisz.message_registry m
    JOIN cleanup_registry_keys k ON k.msgid = m.msgid
    WHERE COALESCE(m.created_at, '-infinity'::timestamptz) < :'cleanup_before'::timestamptz
) s;

SELECT 'documents' AS object_name, count(*) AS candidate_rows FROM cleanup_documents
UNION ALL SELECT 'document_attributes', count(*) FROM mart_egisz.document_attributes a JOIN cleanup_documents d ON d.dwh_id = a.dwh_id
UNION ALL SELECT 'exchange_messages', count(*) FROM cleanup_logids
UNION ALL SELECT 'egisz_exchangelog_parse_attempts', count(*) FROM etl_meta.egisz_exchangelog_parse_attempts p JOIN cleanup_logids l ON l.logid = p.logid
UNION ALL SELECT 'exchangelog', count(*) FROM raw_egisz.exchangelog r JOIN cleanup_logids l ON l.logid = r.logid
UNION ALL SELECT 'egisz_messages', count(*) FROM cleanup_registry_rows
ORDER BY object_name;

\if :apply_cleanup
DELETE FROM mart_egisz.document_attributes a
USING cleanup_documents d
WHERE a.dwh_id = d.dwh_id;

DELETE FROM mart_egisz.documents d
USING cleanup_documents c
WHERE d.dwh_id = c.dwh_id;

DELETE FROM stg_egisz.exchange_messages t
USING cleanup_logids l
WHERE t.logid = l.logid;

DELETE FROM etl_meta.egisz_exchangelog_parse_attempts p
USING cleanup_logids l
WHERE p.logid = l.logid;

DELETE FROM raw_egisz.exchangelog r
USING cleanup_logids l
WHERE r.logid = l.logid;

DELETE FROM raw_egisz.egisz_messages m
USING cleanup_registry_rows c
WHERE m.egmid = c.egmid;

COMMIT;

ANALYZE mart_egisz.documents;
ANALYZE mart_egisz.document_attributes;
ANALYZE stg_egisz.exchange_messages;
ANALYZE etl_meta.egisz_exchangelog_parse_attempts;
ANALYZE raw_egisz.exchangelog;
ANALYZE raw_egisz.egisz_messages;

SELECT serving_egisz.refresh_report_marts();
\else
ROLLBACK;
\echo dry run only; pass -v apply_cleanup=true to delete candidates
\endif
