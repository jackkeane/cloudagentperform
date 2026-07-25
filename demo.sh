#!/usr/bin/env bash
# demo.sh — golden demo + three cloud behaviors.
# Default: mock replay (deterministic, zero GPU/key). --real: same code
# path against any OpenAI-compatible endpoint (LLM_BASE_URL/LLM_MODEL/
# LLM_API_KEY env). No probing, no silent fallback.
set -euo pipefail
cd "$(dirname "$0")"
API=http://localhost:8080
MODE=mock
[[ "${1:-}" == "--real" ]] && MODE=real
LOGS=demo-logs; mkdir -p "$LOGS"

cli() { docker compose exec -T api python -m cli.main --api "$API" "$@"; }

echo "== build sandbox image =="
docker build -q -f sandbox.Dockerfile -t cap-sandbox .
echo "== start platform (LLM_MODE=$MODE, 2 workers) =="
LLM_MODE=$MODE CAP_LEASE_TTL=5 docker compose up -d --build --scale worker=2
for i in $(seq 1 30); do
  curl -fsS "$API/healthz" >/dev/null 2>&1 && break; sleep 1
done
curl -fsS "$API/healthz" >/dev/null 2>&1 \
  || { echo "FAIL: API not healthy after 30s"; docker compose logs api 2>&1 | tail -20; exit 1; }
grep -q synthetic fixtures/trajectories/golden_todo_scan.json \
  && echo "WARN: provisional trajectory in use (record the real one: worker.record)"

echo "== golden demo: TODO scan =="
TID=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
cli follow "$TID" | tee "$LOGS/golden.log"
echo "== artifact =="
curl -fsS "$API/tasks/$TID/artifacts" | tee "$LOGS/artifacts.json"; echo
curl -fsS "$API/tasks/$TID/artifacts/1/report.md" | tee "$LOGS/report.md"
grep -q "mode=$MODE" "$LOGS/golden.log" || { echo "FAIL: provenance banner missing"; exit 1; }
echo "== GOLDEN OK =="

if [[ "$MODE" == "real" ]]; then
  echo "== behaviors skipped in --real (they rely on deterministic replay) =="
  exit 0
fi

echo "== behavior 1: client disconnect -> full replay + live resume =="
B1=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
timeout 3 docker compose exec -T api python -m cli.main --api "$API" follow "$B1" \
  > "$LOGS/b1_first.log" 2>&1 || true          # client killed mid-run
cli follow "$B1" | tee "$LOGS/b1_replay.log"    # reconnect from scratch
grep -q "attempt 1 started" "$LOGS/b1_replay.log" || { echo FAIL-B1-history; exit 1; }
grep -q "succeeded" "$LOGS/b1_replay.log" || { echo FAIL-B1-completion; exit 1; }
echo "== B1 OK: execution is detached from the client =="

echo "== behavior 2: kill worker mid-run -> lease expiry -> attempt 2 =="
B2=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
sleep 2                       # task is now running
docker compose kill worker    # both replicas die
sleep 7                       # lease (5s) expires while nobody runs
docker compose up -d --scale worker=2 worker
cli follow "$B2" | tee "$LOGS/b2.log"
grep -q "attempt 2 started" "$LOGS/b2.log" || { echo FAIL-B2-rerun; exit 1; }
grep -q "succeeded" "$LOGS/b2.log" || { echo FAIL-B2-completion; exit 1; }
echo "== B2 OK: platform survives worker death (honest rerun, attempt=2) =="

echo "== behavior 3: two concurrent tasks, isolated sandboxes =="
B3A=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
B3B=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
sleep 3
DISTINCT=$(docker ps --filter "label=cap.task_id" \
  --format '{{.Label "cap.task_id"}}' | sort -u | wc -l)
[[ "$DISTINCT" -ge 2 ]] || { echo FAIL-B3-parallel-sandboxes; exit 1; }
cli follow "$B3A" > "$LOGS/b3a.log"
cli follow "$B3B" > "$LOGS/b3b.log"
grep -q "succeeded" "$LOGS/b3a.log" && grep -q "succeeded" "$LOGS/b3b.log" \
  || { echo FAIL-B3-completion; exit 1; }
echo "== B3 OK: independent sandboxes and event streams =="

echo "== ALL CLOUD BEHAVIORS PASSED =="
echo "(cleanup: docker compose down -v)"
