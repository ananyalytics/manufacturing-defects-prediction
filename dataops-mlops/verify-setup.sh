#!/usr/bin/env bash
# Day-1 setup verification for DataOps & MLOps training
# Run after `docker compose --profile full up -d` to confirm every service is reachable.

set -e

check() {
  local name=$1
  local url=$2
  if curl -sf -o /dev/null --max-time 5 "$url"; then
    echo "✅ $name reachable at $url"
  else
    echo "❌ $name NOT reachable at $url — check container logs"
  fi
}

echo "== DataOps & MLOps environment check =="
check "Airflow UI"      "http://localhost:8085/health"
check "MLflow UI"       "http://localhost:5000"
check "Prometheus"      "http://localhost:9091/-/healthy"
check "Grafana"         "http://localhost:3000/api/health"
check "FastAPI demo"    "http://localhost:8000/health"
check "Jupyter"         "http://localhost:8889"

echo ""
echo "Login defaults:"
echo "  Airflow  -> admin / admin              (http://localhost:8085)"
echo "  MLflow   -> no auth                    (http://localhost:5000)"
echo "  Grafana  -> admin / admin              (http://localhost:3000)"
echo "  Jupyter  -> token: dataopsmlops2026    (http://localhost:8889)"
echo ""
echo "Reminder: these ports were chosen to avoid your existing Snowflake"
echo "Airflow (8080/8081) and Big Data stack (9090/8082/8888/etc)."
