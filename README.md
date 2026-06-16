# AML-Transaction-Monitoring-on-GCP BOTH batch + streaming + event-driven.

An anti-money-laundering (AML) transaction-monitoring platform on Google Cloud
covering **batch, streaming, and event-driven** ingestion. It runs FINTRAC-style
detection rules, scores customer risk, and prepares a training-ready dataset for
an AML model — gated by data-quality checks.

## Three ingestion modes
| Mode | Purpose | Tech |
|------|---------|------|
| **Batch** | Daily detection, structuring patterns, customer risk rating, reporting | Apache Airflow / Cloud Composer + BigQuery |
| **Streaming** | Real-time screening of each transaction on arrival (large cash, sanctions, high-risk geo) | Pub/Sub + Dataflow (Apache Beam) + BigQuery |
| **Event-driven** | Auto-ingest an upstream file feed the moment it lands | Cloud Storage + Cloud Function (gen2) |

Streaming handles per-transaction red flags instantly; batch handles patterns that
need aggregation over time (e.g. structuring across a day).

## Pipeline (Airflow DAG)

```
ingest_raw
  -> stage
      -> [ rule_lctr | rule_structuring | rule_sanctions | rule_high_risk ]   (parallel)
          -> merge_alerts
              -> customer_risk_rating
                  -> ml_features
                      -> data_quality   (gate: fails the run on mismatch)
```

## Detection rules
| Rule | Logic | Severity |
|------|-------|----------|
| LCTR | single cash transaction ≥ 10,000 (FINTRAC large-cash) | HIGH |
| Structuring | ≥ 3 sub-threshold cash txns by one customer in a day, summing > 10,000 | HIGH |
| Sanctions / name screening | counterparty matches the watchlist | CRITICAL |
| High-risk geography | counterparty in a high-risk country | MEDIUM |

Alerts roll up into a **Customer Risk Rating** (score → LOW/MEDIUM/HIGH band),
and an **ml_features** table provides a labelled, training-ready dataset.

## Stack
Apache Airflow / Cloud Composer, BigQuery, Python, SQL, Google Cloud.
SQL transforms run via `BigQueryInsertJobOperator` (idempotent `CREATE OR REPLACE`);
ingestion and the data-quality gate run as `PythonOperator` tasks.

## Layout
```
dags/aml_monitoring_dag.py        # BATCH: the Airflow DAG
sql/                              # 8 BigQuery transforms (stage, 4 rules, merge, CRR, features)
streaming/aml_stream.py           # STREAMING: Pub/Sub -> Beam -> real-time alerts
streaming/publish_transactions.py # streams live transactions to Pub/Sub
cloud_function/main.py            # EVENT-DRIVEN: GCS file arrival -> BigQuery
data/generate_aml_data.py         # synthetic transactions + watchlist + high-risk countries
```

## Run locally (Airflow against real BigQuery)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install "apache-airflow==2.10.5" "apache-airflow-providers-google" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

export AIRFLOW_HOME=$PWD/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db migrate
airflow connections add google_cloud_default --conn-type google_cloud_platform \
  --conn-extra '{"project": "YOUR_PROJECT"}'
airflow dags test aml_monitoring 2026-06-16
```

## Sample results (1,848 transactions, 300 customers)
- 28 alerts across 4 rules; 27 customers risk-rated (7 HIGH from sanctions hits).
- Training-ready features show clear signal: alerted customers average 2.5 cash
  txns and 0.26 high-risk txns vs 1.2 / 0.0 for non-alerted.
- Data-quality gate passed (alerts reconciliation + CRR customer count).

## Design notes
- **Orchestration**: Airflow models dependencies as a DAG; rules fan out in
  parallel then fan in at `merge_alerts`.
- **Idempotency**: every task is safe to re-run (truncate/replace), so backfills
  and retries don't duplicate data.
- **Quality as a gate**: the final task fails the DAG run if reconciliation breaks.
