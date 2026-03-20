# SaaS GA4 → BigQuery Analytics Pipeline

> End-to-end data pipeline ingesting GA4 web events + product usage events,
> transforming them in **dbt**, orchestrated by **Apache Airflow**, and producing
> a marketing + product performance dashboard — the modern SaaS analytics stack.

---

## Architecture

```
┌──────────┐    ┌───────────┐    ┌─────┐    ┌─────────┐    ┌───────────┐
│  GA4 /   │───▶│ BigQuery  │───▶│ dbt │───▶│ Airflow │───▶│ Dashboard │
│ Simulator│    │ (raw)     │    │     │    │  (DAG)  │    │ Streamlit │
└──────────┘    └───────────┘    └─────┘    └─────────┘    └───────────┘
                                   │                            │
                              ┌────┴─────┐              ┌──────┴──────┐
                              │  Staging  │              │  PQL Scores │
                              │  Intermed.│              │  → CRM Sync │
                              │  Marts    │              │  (HubSpot)  │
                              └──────────┘              └─────────────┘
```

### Data Flow

1. **GA4 Export** → BigQuery daily sharded tables (`events_YYYYMMDD`)
2. **dbt Staging** → flatten nested structs, sessionise, build user profiles
3. **dbt Intermediate** → trial funnel, feature adoption, PLG signals, web performance
4. **dbt Marts** → daily KPIs, trial cohorts, feature funnel, PQL pipeline, page dimension
5. **PQL Scoring** → product-qualified lead scores → CRM (HubSpot / Salesforce)
6. **Dashboard** → Streamlit with 5 tabs (traffic, cohorts, features, PQLs, pages)

---

## Product-Led Growth (PLG) — Explained

> **"The new SaaS growth motion — let product usage replace outbound sales
> as the top-of-funnel signal."**

Traditional B2B SaaS relies on marketing → SDR → demo → close.  
**PLG flips this**: users sign up for a free trial, self-serve onboard, and
their *product behaviour* generates buying signals (PQLs) that replace MQLs.

This pipeline tracks:
- **Trial funnel**: trial_started → feature_used → upgrade_intent → subscription_started
- **Feature adoption breadth**: users who engage with 3+ features by day 14 convert at **38%**
- **Viral signals**: invite_sent events indicate team adoption potential
- **Power user signals**: API key creation + integration connections

### Trial Cohort Insight

> **"Week 2 feature adoption is the strongest predictor of 30-day conversion —
> users who use 3+ features by day 14 convert at 38%, compared to 8% for
> users who use only 1 feature."**

This insight comes from `fct_trial_cohorts` and `fct_feature_funnel` —
the dbt marts that power the cohort analysis tab of the dashboard.

---

## dbt Lineage DAG

```
sources.ga4.events
    │
    ├── stg_ga4__events
    │       │
    │       ├── stg_ga4__sessions
    │       │       │
    │       │       ├── stg_ga4__users
    │       │       │       │
    │       │       │       └── int_plg_signals ──────▶ fct_plg_pipeline
    │       │       │
    │       │       └── fct_saas_web_kpis
    │       │
    │       ├── int_trial_funnel ──┬──▶ fct_trial_cohorts
    │       │                     └──▶ fct_feature_funnel
    │       │
    │       ├── int_feature_adoption ──▶ fct_feature_funnel
    │       │
    │       └── int_web_performance ──▶ dim_pages
```

---

## Project Structure

```
saas-ga4-bigquery-pipeline/
├── data/
│   └── simulate_ga4.py          # GA4 schema simulator (90d × 30k events/day)
├── src/
│   └── pql_scoring.py           # Product-qualified lead scoring
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_ga4__events.sql
│       │   ├── stg_ga4__sessions.sql
│       │   └── stg_ga4__users.sql
│       ├── intermediate/
│       │   ├── int_trial_funnel.sql
│       │   ├── int_feature_adoption.sql
│       │   ├── int_plg_signals.sql
│       │   └── int_web_performance.sql
│       ├── marts/
│       │   ├── fct_saas_web_kpis.sql
│       │   ├── fct_trial_cohorts.sql
│       │   ├── fct_feature_funnel.sql
│       │   ├── fct_plg_pipeline.sql
│       │   └── dim_pages.sql
│       └── schema.yml
├── dags/
│   └── saas_pipeline.py         # Airflow DAG (daily orchestration)
├── dashboard/
│   └── app.py                   # Streamlit dashboard (5 tabs)
├── tests/
│   └── test_pipeline.py         # pytest tests
├── .env.example
├── .gitignore
├── pyproject.toml
└── README.md
```

---

## Setup

### Prerequisites

- Python 3.11+
- Google Cloud Platform project with BigQuery enabled
- Service account with BigQuery Data Editor + Job User roles

### 1. Clone & install

```bash
git clone https://github.com/YOUR_ORG/saas-ga4-bigquery-pipeline.git
cd saas-ga4-bigquery-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env with your GCP project, dataset, and service account path
```

### 3. Generate sample data

```bash
python data/simulate_ga4.py --days 90 --events-per-day 30000
# → generates 2.7M events in data/events_YYYYMMDD.ndjson files
```

### 4. Load to BigQuery

```bash
for f in data/events_*.ndjson; do
  TABLE="events_$(basename $f .ndjson | cut -d_ -f2)"
  bq load --source_format=NEWLINE_DELIMITED_JSON \
    --autodetect \
    $BQ_DATASET.$TABLE $f
done
```

### 5. Run dbt

```bash
cd dbt
dbt deps
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
dbt test
```

### 6. Run PQL scoring

```bash
python src/pql_scoring.py          # demo mode (no BQ)
# or with BQ:
# python -c "from src.pql_scoring import score_and_write_pqls; score_and_write_pqls()"
```

### 7. Launch dashboard

```bash
streamlit run dashboard/app.py
# Runs in demo mode by default; set USE_BIGQUERY=true for live data
```

### 8. Run tests

```bash
pytest tests/ -v
```

---

## Airflow DAG

The daily pipeline (`dags/saas_pipeline.py`) runs at 06:00 UTC:

| Task | Description |
|------|-------------|
| `validate_ga4_partition` | Ensure today's events shard is loaded |
| `run_dbt_staging` | Flatten + clean raw events |
| `run_dbt_intermediate` | Build sessions, trial funnel, PLG signals |
| `run_dbt_marts` | Produce final KPI tables |
| `run_dbt_tests` | Data quality gate — pipeline fails if tests fail |
| `compute_pql_scores` | Score users → write PQL table |
| `send_morning_metrics` | Email daily KPI card via SendGrid |

**SLA**: Pipeline must complete by 08:00 UTC.  
**Alerts**: Slack webhook fires on any task failure.

---

## PQL Scoring Formula

```
Score = (api_key_created × 3)
      + (integrations_connected × 2)
      + (invites_sent × 2)
      + (features_used_count × 1)
      + (sessions_last_7d × 0.5)

PQL threshold: score > 8
```

| Tier | Score Range | Action |
|------|-------------|--------|
| 🔴 Hot | > 15 | Fast-track to sales |
| 🟡 Warm | 8–15 | Schedule demo |
| 🔵 Nurture | 4–8 | Send upgrade email |
| ⚪ Cold | < 4 | Automated nurture |

---

## Cost Estimate

For 90 days of SaaS event data (~2.7M events):

| Item | Estimate |
|------|----------|
| BigQuery storage (raw) | ~0.5 GB → $0.01/month |
| BigQuery storage (dbt models) | ~0.2 GB → $0.004/month |
| Query cost (full dbt run) | ~2 GB scanned → $0.01 per run |
| Daily incremental run | ~50 MB scanned → < $0.001/day |
| **Monthly total** | **~$0.50–$2.00** |

> BigQuery's first 1 TB/month of queries and 10 GB of storage are free.
> This pipeline comfortably fits within the free tier for development.

---

## License

MIT
