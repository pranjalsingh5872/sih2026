#!/usr/bin/env bash
# Idempotent topic bootstrap. Safe to re-run: `--if-not-exists` is a no-op on
# topics that already exist, so this can stay in the compose file permanently.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"

echo "[kafka-init] waiting for broker at ${BOOTSTRAP} ..."
for attempt in $(seq 1 30); do
  if kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
    echo "[kafka-init] broker is up (attempt ${attempt})"
    break
  fi
  if [ "${attempt}" -eq 30 ]; then
    echo "[kafka-init] broker never became reachable" >&2
    exit 1
  fi
  sleep 3
done

# name:partitions:retention_ms
TOPICS=(
  "raw-weather-stream:6:259200000"
  "normalized-incident-stream:6:604800000"
  "incident-dead-letter:1:1209600000"
  "geo-unresolved-incidents:3:604800000"
)

for spec in "${TOPICS[@]}"; do
  IFS=':' read -r name partitions retention <<< "${spec}"
  echo "[kafka-init] ensuring topic ${name} (partitions=${partitions})"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention}" \
    --config "compression.type=producer"
done

echo "[kafka-init] current topics:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list
echo "[kafka-init] done"
