from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
import pendulum
local_tz = pendulum.timezone("Europe/Berlin")

PROJECT = "stay-smart-498122"
REGION = "europe-west3"
BUCKET = "smart-stay-data"
DBT_ACCOUNT_ID = "70506183137732"
DBT_JOB_ID = "70506183132991"

default_args = {
    "owner": "smart_stay",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def script_task(task_id, script):
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"gsutil cp gs://{BUCKET}/scripts/{script} /tmp/{script} && "
            f"python3 /tmp/{script}"
        ),
    )

with DAG(
    dag_id="smart_stay_pipeline",
    default_args=default_args,
    description="Live fetch -> Spark scores (on stored data) -> dbt, automated",
    schedule="0 9,12,15,18,22 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_tasks=2,
    tags=["smart-stay"],
) as dag:

    # FETCH live/small sources only (static data already in GCS)
    fetch_events     = script_task("fetch_events", "fetch_events.py")
    fetch_weather    = script_task("fetch_weather", "fetch_weather.py")
    fetch_punct      = script_task("fetch_punctuality", "fetch_realtime_punctuality.py")
    fetch_punct_hood = script_task("fetch_punctuality_by_hood", "fetch_realtime_punctuality_by_hood.py")

    #SPARK scores (reads pre-loaded transit + airbnb data from GCS) 
    spark_scores = DataprocCreateBatchOperator(
        task_id="spark_scores",
        project_id=PROJECT,
        region=REGION,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET}/scripts/spark_scores.py",
            },
            "runtime_config": {
                "version": "2.2",
                "properties": {
                    "spark.driver.cores": "4",
                    "spark.executor.cores": "4",
                    "spark.executor.instances": "2",
                },
            },
        },
        batch_id="smart-stay-scores-{{ ts_nodash | lower }}",
    )

    # EVENTS scoring to BQ 
    score_events = script_task("score_events", "score_events.py")

    # dbt build 
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
        TOKEN=$(gcloud secrets versions access latest --secret=dbt-api-token)
        RUN_ID=$(curl -s -X POST \
          "https://mk643.us1.dbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/" \
          -H "Authorization: Token $TOKEN" -H "Content-Type: application/json" \
          -d '{{"cause": "Triggered by Composer"}}' \
          | python3 -c "import sys, json; print(json.load(sys.stdin)['data']['id'])")
        echo "dbt run id: $RUN_ID"
        while true; do
          STATUS=$(curl -s \
            "https://mk643.us1.dbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/$RUN_ID/" \
            -H "Authorization: Token $TOKEN" \
            | python3 -c "import sys, json; print(json.load(sys.stdin)['data']['status'])")
          echo "dbt status: $STATUS"
          if [ "$STATUS" = "10" ]; then echo "SUCCESS"; break; fi
          if [ "$STATUS" = "20" ]; then echo "FAILED"; exit 1; fi
          if [ "$STATUS" = "30" ]; then echo "CANCELLED"; exit 1; fi
          sleep 30
        done
        """,
    )

    # DEPENDENCIES 
    fetch_events >> score_events
    [spark_scores, score_events, fetch_weather, fetch_punct, fetch_punct_hood] >> dbt_build