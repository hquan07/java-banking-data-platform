from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


BUSINESS_TIMEZONE = "Asia/Ho_Chi_Minh"
BATCH_DATE = (
    "{{ dag_run.conf.get('date', "
    "data_interval_start.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d')) "
    "if dag_run and dag_run.conf else "
    "data_interval_start.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }}"
)
JAVA_BATCH = "java -jar /opt/banking/batch-processor.jar"


def batch_task(task_id: str, job: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f'{JAVA_BATCH} --job={job} --date="$BATCH_DATE"',
        env={"BATCH_DATE": BATCH_DATE},
        append_env=True,
        do_xcom_push=False,
    )


with DAG(
    dag_id="banking_daily_transactions",
    description="Finalize warehouse daily transactions and publish the authoritative Gold partition.",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=BUSINESS_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "banking-data-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    tags=["banking", "batch", "gold"],
) as dag:
    check_source_partition = batch_task("check_source_partition", "check-daily-source")
    run_java_daily_batch = batch_task("run_java_daily_batch", "daily-transactions")
    validate_aggregates = batch_task("validate_aggregates", "validate-daily-transactions")
    publish_report_metadata = batch_task("publish_report_metadata", "publish-report-metadata")

    check_source_partition >> run_java_daily_batch >> validate_aggregates >> publish_report_metadata
