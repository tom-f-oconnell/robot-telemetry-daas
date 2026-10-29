# CI/CD

Five GitHub Actions workflows under `.github/workflows/`.

| Workflow | Trigger | Jobs |
|---|---|---|
| `ci.yml` | push, PR | **lint** (ruff) · **unit** (py 3.11 / 3.12; `pytest -m "not spark and not airflow and not integration"`) · **spark** (Temurin 17, `pip install .[spark]`, `pytest -m spark`) · **airflow** (`pip install .[airflow]` with constraints, DAG integrity tests) · **docs** (`mkdocs build --strict`) |
| `contracts.yml` | PR touching `contracts/**` or `rtdaas/contracts/**` | `rtdaas-contracts gen` + `git diff --exit-code`; `rtdaas-contracts check --base origin/main --mode BACKWARD` |
| `images.yml` | push to `main`, tags | buildx matrix over api / spark / airflow / sim / bridge → GHCR, tagged `<sha>` and `latest` |
| `integration.yml` | nightly + manual | kind cluster → Strimzi + 1-broker Kafka + Apicurio + MinIO → load sim + api images → run sim 60 s → assert `/api/health` and `/api/robots` |
| `deploy.yml` | after `images.yml` on `main` | `kustomize edit set image` for the five images in `infra/k8s/manifests/rtdaas`, commit `[skip ci]`; Argo CD syncs |

## Test tiers

| Tier | Needs | Marker | Where it runs |
|---|---|---|---|
| Unit | Python only | (none) | laptop, every CI run |
| Spark | pyspark + Java | `spark` | CI `spark` job |
| Airflow | apache-airflow | `airflow` | CI `airflow` job |
| Integration | running services | `integration` | nightly kind cluster |

Data-quality logic is tested at the unit tier (pandas adapter), so a broken check never reaches
the cluster; schema changes are tested at the contract tier before they reach the registry.

## Branch protection suggested

Require `ci / unit`, `ci / lint`, `contracts / check` on PRs to `main`. The Spark and Airflow
jobs are slower (dependency installs) and can be required once caching is warm.
