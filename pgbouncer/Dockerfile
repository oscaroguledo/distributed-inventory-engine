FROM debian:bookworm-slim

RUN useradd --create-home --uid 10001 appuser \
    && apt-get update \
    && apt-get install -y --no-install-recommends pgbouncer gettext-base netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY pgbouncer.ini.template /etc/pgbouncer/pgbouncer.ini.template
COPY userlist.txt.template /etc/pgbouncer/userlist.txt.template
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && chown -R appuser:appuser /etc/pgbouncer

USER appuser
EXPOSE 6432
ENTRYPOINT ["/entrypoint.sh"]
