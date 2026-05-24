#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_JAR="${SCRIPT_DIR}/target/mango-swarm-reference-email-app-0.1.0-SNAPSHOT.jar"

echo "Running clean install from parent project..."
(
  cd "${SCRIPT_DIR}/../.."
  mvn clean install
)

echo "Rebuilding reference app executable jar..."
(
  cd "${SCRIPT_DIR}"
  mvn -DskipTests package
)

cd "${SCRIPT_DIR}"

rm -f log.1 log.2 instance1.pid instance2.pid

nohup java -jar "${APP_JAR}" \
  --server.port=8080 \
  --reference.email.seed-on-startup=true \
  > log.1 2>&1 &
echo $! > instance1.pid

nohup java -jar "${APP_JAR}" \
  --server.port=8081 \
  --reference.email.seed-on-startup=false \
  > log.2 2>&1 &
echo $! > instance2.pid

echo "Started instance 1 (pid $(cat instance1.pid)) -> ${SCRIPT_DIR}/log.1"
echo "Started instance 2 (pid $(cat instance2.pid)) -> ${SCRIPT_DIR}/log.2"

echo "Instances running for 90 seconds..."
sleep 90

echo "Stopping instances..."
kill "$(cat instance1.pid)" "$(cat instance2.pid)" 2>/dev/null || true
wait "$(cat instance1.pid)" "$(cat instance2.pid)" 2>/dev/null || true

echo "Stopped. Logs available at:"
echo "  ${SCRIPT_DIR}/log.1"
echo "  ${SCRIPT_DIR}/log.2"
