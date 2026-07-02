#!/usr/bin/env sh
# Flink smoke test against a Meridian server on localhost:8181 with MinIO
# on localhost:9000. See README.md for prerequisites (warehouse + bucket).
set -eu

cd "$(dirname "$0")"

./fetch-jars.sh
./setup.sh

docker compose up -d

# Wait for the taskmanager to register with the jobmanager.
echo "waiting for the Flink cluster to come up..."
tries=0
until docker exec meridian-flink-jobmanager bash -c \
    'exec 3<>/dev/tcp/localhost/8081 && printf "GET /taskmanagers HTTP/1.0\r\n\r\n" >&3 && cat <&3' \
    2>/dev/null | grep -q '"slotsNumber":[1-9]'; do
  tries=$((tries + 1))
  if [ "$tries" -gt 60 ]; then
    echo "Flink cluster did not become ready in 120s" >&2
    docker compose logs --tail 50 >&2
    exit 1
  fi
  sleep 2
done
echo "cluster ready."

echo "=== batch smoke ==="
docker exec meridian-flink-jobmanager \
    /opt/flink/bin/sql-client.sh -i /opt/sql/00_catalog.sql -f /opt/sql/10_batch_smoke.sql

echo "=== streaming smoke ==="
docker exec meridian-flink-jobmanager \
    /opt/flink/bin/sql-client.sh -i /opt/sql/00_catalog.sql -f /opt/sql/20_streaming_smoke.sql

echo "done. Tear down with: docker compose down"
