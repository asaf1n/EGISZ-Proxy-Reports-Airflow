FROM metabase/metabase:v0.62.1

LABEL org.opencontainers.image.title="egisz-integration-metabase"
LABEL org.opencontainers.image.description="Metabase OSS for EGISZ integration service reports"

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

USER root
RUN apk add --no-cache jq curl postgresql-client bash shadow \
    && groupadd -g 1500 metabase-local \
    && useradd -u 1500 -g 1500 -m -d /home/metabase-local -s /sbin/nologin metabase-local \
    && chown -R 1500:1500 /app /tmp

COPY --chown=1500:1500 metabase/include/mb_list.sh /app/include/mb_list.sh
COPY --chown=1500:1500 metabase/entrypoint.sh /app/entrypoint.sh
COPY --chown=1500:1500 metabase/provision.sh /app/provision.sh
# bust COPY cache when provisioning scripts change
COPY --chown=1500:1500 metabase/setup-dashboards.sh /app/setup-dashboards.sh
COPY --chown=1500:1500 metabase/smoke-metabase-ui.sh /app/smoke-metabase-ui.sh
COPY --chown=1500:1500 metabase/sync-models.sh /app/sync-models.sh
COPY --chown=1500:1500 metabase_models/ /app/metabase_models/
COPY --chown=1500:1500 metabase_dashboards/ /app/metabase_dashboards/

RUN chmod +x /app/entrypoint.sh /app/provision.sh /app/setup-dashboards.sh /app/sync-models.sh /app/smoke-metabase-ui.sh /app/include/mb_list.sh \
    && find /app/metabase_dashboards -maxdepth 1 -name '*.json' -type f | LC_ALL=C sort | xargs sha256sum | sha256sum > /app/metabase_dashboards/.manifest.sha256 || true

USER 1500:1500

HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
  CMD curl --fail --silent --output /dev/null http://localhost:3000/api/health || exit 1

ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]
