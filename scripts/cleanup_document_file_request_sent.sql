\set ON_ERROR_STOP on

\if :{?apply}
\else
\set apply false
\endif

CREATE TEMP TABLE false_document_file_request_sent AS
SELECT
    d.dwh_id,
    d.request_logid,
    d.first_sent_at,
    d.jid,
    tx.xml_emdr_id,
    tx.xml_local_uid,
    tx.xml_semd_code
FROM mart_egisz.documents d
JOIN stg_egisz.exchange_messages tx ON tx.logid = d.request_logid
WHERE d.status = 'sent'
  AND tx.source_action = 'getDocumentFile'
  AND NULLIF(btrim(tx.xml_emdr_id), '') IS NOT NULL;

SELECT
    count(*) AS candidates,
    min(first_sent_at) AS min_first_sent_at,
    max(first_sent_at) AS max_first_sent_at
FROM false_document_file_request_sent;

SELECT
    date_trunc('day', first_sent_at AT TIME ZONE 'Europe/Moscow')::date AS first_sent_day_msk,
    count(*) AS candidates
FROM false_document_file_request_sent
GROUP BY 1
ORDER BY 1;

\if :apply
BEGIN;

DELETE FROM mart_egisz.document_attributes a
USING false_document_file_request_sent c
WHERE a.dwh_id = c.dwh_id;

DELETE FROM mart_egisz.documents d
USING false_document_file_request_sent c
WHERE d.dwh_id = c.dwh_id;

ANALYZE mart_egisz.documents;
ANALYZE mart_egisz.document_attributes;
ANALYZE stg_egisz.exchange_messages;

COMMIT;

SELECT 'applied' AS cleanup_status, count(*) AS remaining_candidates
FROM mart_egisz.documents d
JOIN stg_egisz.exchange_messages tx ON tx.logid = d.request_logid
WHERE d.status = 'sent'
  AND tx.source_action = 'getDocumentFile'
  AND NULLIF(btrim(tx.xml_emdr_id), '') IS NOT NULL;
\else
SELECT 'dry-run' AS cleanup_status, count(*) AS candidates
FROM false_document_file_request_sent;
\endif
