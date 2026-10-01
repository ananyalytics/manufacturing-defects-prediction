# DataOps & MLOps Training Stack — Setup Guide
Jain University x HCL GUVI | Airflow + MLflow + Prometheus + Grafana + FastAPI + Jupyter + DVC

A self-contained Docker Compose stack for the DataOps & MLOps corporate
training course — covers hands-on labs for Modules 2 through 4.

## Folder Setup (Windows)

1. Create a folder, e.g. `C:\dataops-mlops-docker\`
2. Place `docker-compose.yml`, the `prometheus\` folder, and the `fastapi-demo\`
   folder inside it, matching the structure in this zip.
3. Open PowerShell / CMD, `cd C:\dataops-mlops-docker`

## What's in the Stack

| Service | Purpose | Module |
|---|---|---|
| Airflow (webserver + scheduler + Postgres) | Workflow orchestration | Module 2 |
| DVC tools container | Data versioning (CLI only, no UI) | Module 2 |
| MLflow | Experiment tracking & model registry | Module 3 |
| FastAPI demo | Model serving endpoint | Module 4 |
| Prometheus | Metrics collection | Module 4 |
| Grafana | Monitoring dashboards | Module 4 |
| Jupyter | Hands-on notebooks for all modules | Module 2–3 |

## Port Map

| Port | Service |
|---|---|
| 5434 | Airflow Postgres metadata DB |
| 8085 | Airflow webserver — login `admin` / `admin` |
| 5000 | MLflow tracking server |
| 9091 | Prometheus |
| 3000 | Grafana — login `admin` / `admin` |
| 8000 | FastAPI model-serving demo |
| 8889 | Jupyter Notebook — token: `dataopsmlops2026` |
| — | DVC tools container (no port — CLI only, access via `docker exec`) |

If any of these ports are already in use by something else on your machine,
just change the left-hand number in `docker-compose.yml` (e.g. `"8085:8080"`
→ `"8095:8080"`) and use the new number to connect.

## Commands

```bash
# Sessions 3-5: pipeline automation only (Airflow + Jupyter + DVC, lighter)
docker-compose --profile pipelines up -d

# Sessions 6-9: full stack incl. MLflow, Prometheus, Grafana, FastAPI
docker-compose --profile full up -d

# Check what's running
docker-compose ps

# Stop everything for the day (keeps data)
docker-compose --profile full stop

# Full cleanup (WARNING: deletes Airflow/MLflow data)
docker-compose --profile full down -v
```

## Using DVC

DVC is a command-line tool, not a running service, so there's no browser
URL for it. Run commands from inside its dedicated container:

```bash
docker exec -it dataops_dvc_tools bash
cd /workspace
dvc --version
git --version
```

`/workspace` inside this container and `/home/jovyan/work` inside Jupyter
both point to the same host `notebooks/` folder, so files versioned here
are the exact same files visible in Jupyter's file browser. Typical flow:

```bash
git init
dvc init
mkdir -p ../dvc-storage
dvc remote add -d localstorage ../dvc-storage
dvc add data/orders_validated.csv
git add data/orders_validated.csv.dvc data/.gitignore .dvc
git commit -m "Version orders_validated.csv v1"
dvc push
```

## Day-1 Verification Checklist

Run after `docker-compose --profile pipelines up -d` and wait ~1-2 minutes:

1. **Airflow UI** — open `http://localhost:8085` → log in with `admin` / `admin`
2. **Jupyter Notebook** — open `http://localhost:8889` → token `dataopsmlops2026`
3. **DVC tools** — `docker exec -it dataops_dvc_tools dvc --version` should return a version number

After `docker-compose --profile full up -d`, additionally check:

4. **MLflow UI** — `http://localhost:5000` → should show an empty experiments list
5. **Prometheus** — `http://localhost:9091` → Status → Targets should show `fastapi_demo` as UP
6. **Grafana** — `http://localhost:3000` → log in with `admin` / `admin`
7. **FastAPI demo** — `http://localhost:8000/health` → should return `{"status":"ok"}`

Or run `verify-setup.sh` (Git Bash / WSL) to check all web endpoints in one shot.

## Common Issues

| Symptom | Fix |
|---|---|
| Containers restart-looping | Docker Desktop RAM limit too low — increase to 8GB+ in Docker Desktop → Settings → Resources |
| Port already allocated | Run `docker ps -a` to find what else is using that port, or change the port mapping in `docker-compose.yml` |
| Airflow webserver unhealthy | `airflow-init` needs to finish first — check `docker logs dataops_airflow_init` |
| MLflow shows a sqlite/database error | Confirm the `mlflow` service's volume mount points to a folder Docker Desktop has file-sharing permission for (Settings → Resources → File Sharing) |
| Can't reach Grafana dashboards | Prometheus must be added as a data source manually on first login: Connections → Data sources → Prometheus → URL `http://prometheus:9090` (internal port, not 9091) |
| `dvc` or `git` command not found | Container may not have finished building — run `docker-compose --profile full up -d dvc-tools` again and check `docker logs dataops_dvc_tools` |
