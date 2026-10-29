#!/usr/bin/env sh
set -eu

if [ ! -f .env ]; then
  cp .env.example .env
  password=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
  api_key=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
  sed -i "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=${password}/" .env
  sed -i "s/^MAO_API_KEY=.*/MAO_API_KEY=${api_key}/" .env
fi

api_key=$(sed -n 's/^MAO_API_KEY=//p' .env | head -n 1)

docker compose up -d --build

for name in mao-db mao-redis mao-engine-1 mao-engine-2 mao-gateway mao-task-executor mao-autoheal; do
  attempts=0
  state=""
  while [ "$attempts" -lt 60 ]; do
    state=$(docker inspect "$name" --format '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' 2>/dev/null || true)
    if [ "$state" = "running/healthy" ]; then
      break
    fi
    attempts=$((attempts + 1))
    sleep 3
  done
  if [ "$state" != "running/healthy" ]; then
    echo "$name is not healthy: $state" >&2
    exit 1
  fi
done

cert_state=$(docker inspect mao-cert-generator --format '{{.State.Status}}/{{.State.ExitCode}}' 2>/dev/null || true)
if [ "$cert_state" != "exited/0" ]; then
  echo "certificate generator did not exit cleanly: $cert_state" >&2
  exit 1
fi

status=$(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:8090/events/cursor?event_type=TASK_CREATED')
if [ "$status" != "401" ]; then
  echo "expected HTTP 401 without API key, got $status" >&2
  exit 1
fi

curl -fsS -H "X-API-Key: ${api_key}" \
  'http://127.0.0.1:8090/events/cursor?event_type=TASK_CREATED' >/dev/null

curl -kfsS 'https://127.0.0.1:8443/health' >/dev/null

if ! docker compose exec -T engine-1 wget -qO- http://127.0.0.1:8090/metrics | grep -q 'mao_http_requests_total'; then
  echo "metrics endpoint does not expose mao_http_requests_total" >&2
  exit 1
fi

docker compose exec -T task-executor python healthcheck.py
docker compose exec -T task-executor sh -c 'python -m unittest discover -s tests -v 2>&1'
docker compose exec -T task-executor python -u trigger_task.py

if ! docker compose exec -T redis redis-cli XINFO GROUPS mao:tasks | grep -q 'mao-agents'; then
  echo "Redis consumer group mao-agents is missing" >&2
  exit 1
fi

pending=$(docker compose exec -T redis redis-cli XPENDING mao:tasks mao-agents | head -n 1 | tr -d '\r')
if [ "$pending" != "0" ]; then
  echo "task stream has $pending pending entries" >&2
  exit 1
fi

echo STACK_VERIFICATION_OK
