#!/bin/sh
set -eu

envsubst < /etc/pgbouncer/pgbouncer.ini.template > /etc/pgbouncer/pgbouncer.ini
envsubst < /etc/pgbouncer/userlist.txt.template > /etc/pgbouncer/userlist.txt
exec pgbouncer /etc/pgbouncer/pgbouncer.ini
