-- ============================================================================
-- Одноразовая точечная переклассификация после актуализации dim_error_rules
-- (2026-07): вместо полного transform (~20 ч) пересчитываются только документы,
-- задетые новыми/изменёнными правилами. Входы воспроизводимы: те же
-- (error_code, message, msgtext), что transform передавал в build_errors_json.
-- Идемпотентно: guard IS DISTINCT FROM, повторный запуск — no-op.
-- После запуска: REFRESH MATERIALIZED VIEW CONCURRENTLY public.rpt_error_breakdown;
-- ============================================================================

BEGIN;

WITH target_tx AS (
    SELECT t.logid, t.log_date, t.dwh_id,
           public.build_errors_json('error', t.error_code, t.message, r.msgtext) AS ej
    FROM public.transactions t
    JOIN public.exchangelog_raw r
      ON r.logid = t.logid
     AND r.createdate >= t.log_date - interval '1 day'
     AND r.createdate <  t.log_date + interval '1 day'
    WHERE t.status = 'error'
      AND COALESCE(t.error_code, '') <> 'INTEGRATION_LOGSTATE_3'
      AND (
            upper(COALESCE(t.error_code, '')) IN (
                'INVALID_CONTENT', 'DOC_DATE_MISMATCH_CERT_NOT_AFTER',
                'DOC_DATE_MISMATCH_CERT_NOT_BEFORE', 'INVALID_DOCTOR_NAME',
                'RUNTIME_ERROR', 'INTERNAL_ERROR',
                'CA_INACCESSIBILITY', 'CA_UNAVAILABLE',
                'ASYNC_RESPONSE_TIMEOUT', 'TIMEOUT')
         OR t.message ~* 'Недопустимые символы в имени'
         OR t.error_type ~ '(^| · )(Код: |Неизвестная ошибка)'
         OR t.error_type LIKE '%повторите отправку позже%'
         OR t.error_type LIKE '%ИЭМК: некорректный идентификатор документа%'
      )
),
dict AS (SELECT DISTINCT ej FROM target_tx),
interp AS (
    SELECT ej,
           public.error_classify(ej) AS new_type,
           public.error_messages_row(ej) AS new_text
    FROM dict
),
upd_tx AS (
    UPDATE public.transactions t
    SET error_type = i.new_type,
        error_json_text = i.new_text
    FROM target_tx x
    JOIN interp i ON i.ej = x.ej
    WHERE t.logid = x.logid AND t.log_date = x.log_date
      AND (t.error_type IS DISTINCT FROM i.new_type
           OR t.error_json_text IS DISTINCT FROM i.new_text)
    RETURNING t.logid, t.dwh_id, t.error_type, t.error_json_text
)
UPDATE public.documents d
SET error_types = u.error_type,
    error_text = u.error_json_text,
    updated_at = now()
FROM upd_tx u
WHERE d.dwh_id = u.dwh_id
  AND d.result_logid = u.logid
  AND d.status = 'async_error'
  AND (d.error_types IS DISTINCT FROM u.error_type
       OR d.error_text IS DISTINCT FROM u.error_json_text);

COMMIT;
