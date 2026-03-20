"""
SaaS GA4 → BigQuery daily pipeline DAG
=======================================
Daily schedule:
  1. validate_ga4_partition  — ensure today's events shard exists
  2. run_dbt_staging         — flatten & clean
  3. run_dbt_intermediate    — sessions, trial funnel, PLG signals
  4. run_dbt_marts           — final KPI tables
  5. run_dbt_tests           — data quality gate
  6. compute_pql_scores      — Python PQL scoring
  7. send_morning_metrics    — email daily KPI card

SLA: pipeline must complete by 08:00 UTC.
Slack alert on any task failure.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack_webhook import (
    SlackWebhookNotifier,
)

# ──────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
BQ_DATASET = os.environ.get("BQ_DATASET", "ga4_analytics")
DBT_DIR = os.environ.get("DBT_DIR", "/opt/airflow/dbt")
SLACK_CONN_ID = "slack_webhook"
SLA_DEADLINE = timedelta(hours=8)  # must finish by 08:00 UTC

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": SLA_DEADLINE,
}


# ──────────────────────────────────────────────────────────
# Slack failure callback
# ──────────────────────────────────────────────────────────
def _slack_failure_callback(context: dict) -> None:
    """Send Slack alert on task failure."""
    notifier = SlackWebhookNotifier(
        slack_webhook_conn_id=SLACK_CONN_ID,
        text=(
            f":red_circle: *Pipeline failure*\n"
            f"DAG: `{context['dag'].dag_id}`\n"
            f"Task: `{context['task_instance'].task_id}`\n"
            f"Execution: {context['execution_date']}\n"
            f"Log: {context['task_instance'].log_url}"
        ),
    )
    notifier.notify(context=context)


# ──────────────────────────────────────────────────────────
# Python callables
# ──────────────────────────────────────────────────────────
def _validate_ga4_partition(**context) -> None:
    """Check that today's GA4 events shard exists in BigQuery."""
    from google.cloud import bigquery

    ds = context["ds_nodash"]  # YYYYMMDD
    client = bigquery.Client(project=GCP_PROJECT)
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.events_{ds}"

    try:
        table = client.get_table(table_ref)
        row_count = table.num_rows
        if row_count == 0:
            raise ValueError(f"Partition {table_ref} exists but has 0 rows")
        print(f"✓ {table_ref} has {row_count:,} rows")
    except Exception as exc:
        raise RuntimeError(
            f"GA4 partition validation failed for {table_ref}: {exc}"
        ) from exc


def _compute_pql_scores(**context) -> None:
    """Run PQL scoring and write results to BigQuery."""
    import sys

    sys.path.insert(0, "/opt/airflow/src")
    from pql_scoring import score_and_write_pqls

    score_and_write_pqls(
        project_id=GCP_PROJECT,
        dataset=BQ_DATASET,
    )


def _send_morning_metrics(**context) -> None:
    """Send daily KPI email via SendGrid."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    ds = context["ds"]  # YYYY-MM-DD
    yesterday = context["prev_ds"]

    query = f"""
        with today as (
            select
                sum(sessions) as sessions,
                sum(trial_starts) as trials,
                sum(paid_conversions) as conversions,
                array_agg(
                    struct(channel_group, sessions)
                    order by sessions desc limit 3
                ) as top_channels
            from `{GCP_PROJECT}.{BQ_DATASET}.fct_saas_web_kpis`
            where date_day = '{ds}'
        ),
        yesterday as (
            select
                sum(sessions) as sessions,
                sum(trial_starts) as trials,
                sum(paid_conversions) as conversions
            from `{GCP_PROJECT}.{BQ_DATASET}.fct_saas_web_kpis`
            where date_day = '{yesterday}'
        )
        select
            t.sessions, t.trials, t.conversions,
            y.sessions as y_sessions, y.trials as y_trials, y.conversions as y_conversions
        from today t, yesterday y
    """
    rows = list(client.query(query).result())
    if not rows:
        print("No KPI data available — skipping email")
        return

    row = rows[0]
    subject = f"📊 Daily SaaS KPIs — {ds}"
    body = (
        f"<h2>Daily Metrics ({ds})</h2>"
        f"<table>"
        f"<tr><th>Metric</th><th>Today</th><th>Yesterday</th></tr>"
        f"<tr><td>Sessions</td><td>{row.sessions:,}</td><td>{row.y_sessions:,}</td></tr>"
        f"<tr><td>Trial Starts</td><td>{row.trials:,}</td><td>{row.y_trials:,}</td></tr>"
        f"<tr><td>Conversions</td><td>{row.conversions:,}</td><td>{row.y_conversions:,}</td></tr>"
        f"</table>"
    )

    sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
    if not sendgrid_api_key:
        print("SENDGRID_API_KEY not set — printing metrics to log instead")
        print(body)
        return

    import sendgrid
    from sendgrid.helpers.mail import Content, Email, Mail, To

    sg = sendgrid.SendGridAPIClient(api_key=sendgrid_api_key)
    message = Mail(
        from_email=Email(os.environ.get("ALERT_EMAIL_FROM", "data@example.com")),
        to_emails=To(os.environ.get("ALERT_EMAIL_TO", "team@example.com")),
        subject=subject,
        html_content=Content("text/html", body),
    )
    response = sg.client.mail.send.post(request_body=message.get())
    print(f"SendGrid response: {response.status_code}")


# ──────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────
with DAG(
    dag_id="saas_ga4_bigquery_pipeline",
    description="Daily GA4 → BigQuery → dbt pipeline for SaaS analytics",
    schedule="0 6 * * *",  # 06:00 UTC daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=_slack_failure_callback,
    tags=["ga4", "dbt", "saas", "marketing"],
    max_active_runs=1,
) as dag:

    validate_ga4_partition = PythonOperator(
        task_id="validate_ga4_partition",
        python_callable=_validate_ga4_partition,
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_DIR} && dbt run --select staging --profiles-dir {DBT_DIR}"
        ),
    )

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=(
            f"cd {DBT_DIR} && dbt run --select intermediate --profiles-dir {DBT_DIR}"
        ),
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {DBT_DIR} && dbt run --select marts --profiles-dir {DBT_DIR}"
        ),
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}"
        ),
    )

    compute_pql_scores = PythonOperator(
        task_id="compute_pql_scores",
        python_callable=_compute_pql_scores,
    )

    send_morning_metrics = PythonOperator(
        task_id="send_morning_metrics",
        python_callable=_send_morning_metrics,
    )

    # ── task dependencies ──
    (
        validate_ga4_partition
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_marts
        >> run_dbt_tests
        >> compute_pql_scores
        >> send_morning_metrics
    )
