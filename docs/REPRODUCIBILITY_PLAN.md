# Reproducibility test plan

Goal: prove the README is sufficient. Anyone with Docker + Power BI Desktop
should clone the repo, follow the README literally, and reach a working
dashboard in under 30 minutes.

## Setup (5 min)

1. Pick a fresh location, **not** the working repo:
   ```powershell
   cd $env:USERPROFILE\Desktop\repro-test
   ```
2. Confirm you do **not** have a `.env` file lying around there:
   ```powershell
   Test-Path .env       # should print False
   ```
3. Tear down any running stack from the working repo to free the ports:
   ```powershell
   cd <working repo>
   docker compose down -v
   cd <repro-test dir>
   ```

## The dry run (target: < 30 min wall clock)

Open a timer. Walk through `README.md §3` literally, copy-pasting commands.
Do **not** rely on anything you remember; if a step is missing from the
README, that's a bug.

### Checkpoints (record wall-clock time at each)

| Checkpoint                                        | Expected | Notes |
| ------------------------------------------------- | -------- | ----- |
| `git clone` finished                              | ~30 s    |       |
| `cp env.example .env` and edits done              | ~1 min   | Default values should work for evaluation. |
| `docker compose up -d --build` finished (first time, no cache) | ~5–8 min | Image build downloads Airflow base + Java + JDBC + pip deps. |
| `airflow-init` exited 0                           | ~30 s after build | Check with `docker compose logs airflow-init`. |
| Webserver + scheduler healthy                     | ~60 s after init | `docker compose ps`. |
| Airflow UI reachable at <http://localhost:8081>   | immediate |  |
| Login works with the credentials from `.env`      | immediate |       |
| DAG `qversity_pipeline` visible and toggleable    | immediate |       |
| DAG triggered, all tasks green                    | ~3–5 min | The bronze load is the longest single task. |
| `gold.mart_customer_360` query returns ~5,000 rows | < 5 s |       |
| Power BI connects to `localhost:5432 / gold`      | < 30 s    |       |
| Dashboard renders all 4 pages with data           | ~20 s     |       |

Total target: < 30 min. If any step takes longer than its expected slot,
note it; that is a candidate fix for the README before the final tag.

## Failure modes to expect and document

These are the most likely bugs an external runner hits. For each, decide
upfront whether the README addresses it or whether you need to add a
"Troubleshooting" subsection.

| Symptom | Likely cause | Where to fix |
| ------- | ------------ | ------------ |
| `airflow-init` loops or never reports completed | `AIRFLOW_UID` not set, or the host folder cannot be written by UID 50000 | README should call out that `env.example` already sets `AIRFLOW_UID=50000`; mention `AIRFLOW_UID=$(id -u)` for Linux hosts. |
| Webserver healthcheck fails after init | Port 8081 already in use on the host | README §3.2 already maps 8081; add a one-line tip: "if 8081 is taken, change `ports:` in docker-compose to `"8082:8080"`." |
| `dbt run` complains about missing dbt-utils | First-run does not auto-install packages | The DAG handles `dbt deps` implicitly via `dbt run`; if running manually, document `dbt deps` in §3.4. |
| Power BI cannot connect ("server unreachable") | The connector resolves `localhost` to `::1` (IPv6) but Postgres only listens on IPv4 in the container | Suggest using `127.0.0.1` instead of `localhost`. |
| Power BI auth fails | User typed `AIRFLOW_ADMIN_PASSWORD` instead of `POSTGRES_PASSWORD` | Already documented in §3.5 but worth bolding. |
| `flatten_*` Spark task fails with `KeyError: POSTGRES_USER` | `BashOperator` lost the env var | Already fixed: `SPARK_ENV` dict in the DAG passes them explicitly. If this fires, the `.env` wasn't loaded — point at `cp env.example .env`. |

## After the dry run

1. Note every README ambiguity in a TODO list.
2. Edit the README; commit with a descriptive message.
3. Repeat the dry run from step 1 if any edit was non-trivial.
4. Optional but recommended: re-run the dry run on a **different** host
   (a colleague's laptop, a GitHub Codespace, a fresh VM). Different host
   = different surface-area bug. If Codespace, use the Universal devcontainer.

## Codespaces shortcut (if running from a second machine isn't possible)

```yaml
# .devcontainer/devcontainer.json — minimal, ephemeral
{
  "name": "qversity-data-2026",
  "image": "mcr.microsoft.com/devcontainers/universal:2-linux",
  "features": {
    "ghcr.io/devcontainers/features/docker-in-docker:2": {}
  },
  "postCreateCommand": "cp env.example .env && docker compose up -d --build"
}
```

A Codespace will not run Power BI Desktop (Windows-only), so cap the
Codespace test at "DAG green + gold marts queryable from psql". Power BI
end-to-end stays a Windows-host check.

## Commit message templates for the night block

```
chore: README clarifications surfaced by reproducibility dry run
chore: troubleshooting subsection for Power BI loopback / port collisions
docs: reproducibility tested from clean clone — ~XX min, all green
```
