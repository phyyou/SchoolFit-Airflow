"""
### Meal data ETL DAG Documentation
This ETL dag stores Korean school meal data in Data Lake (my S3).
For parallel processing, there are 17 ETL flows that are divided into Korean regional codes.
Extract Task downloads json data through NEIS OpenAPI and then XCom push it.
Transform Task discards the first row, which is data that is not needed.
Load Task stores data in the my S3 bucket with the date as the directory.
"""
import json
from datetime import datetime
from textwrap import dedent
from typing import List, Optional
from urllib import parse
from functools import partial
import logging

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

ATPT_OFCDC_SC_CODES = [
    "B10",
    "C10",
    "D10",
    "E10",
    "F10",
    "G10",
    "H10",
    "I10",
    "J10",
    "K10",
    "M10",
    "N10",
    "P10",
    "Q10",
    "R10",
    "S10",
    "T10",
]
MEAL_NAMES = ["조식", "중식", "석식"]
TASK_TYPES = ["extract", "transform", "load"]

BUCKET_NAME = Variable.get("AWS_SCHOOLFIT_DATA_BUCKET")


def get_meal_form_data(
    code: str,
    to_date: datetime,
    end_date: datetime,
    meal_name: Optional[str] = None,
) -> bytes:
    if meal_name:
        return bytes(
            "rows=100&infId=OPEN17320190722180924242823&infSeq=1&downloadType=J&"
            + f"ATPT_OFCDC_SC_CODE={code}&SCHUL_NM=&MMEAL_SC_NM={parse.quote(meal_name)}"
            + f"&MLSV_YMD={to_date.strftime('%Y%m%d')}&MLSV_YMD={end_date.strftime('%Y%m%d')}",
            "utf-8",
        )
    return bytes(
        "rows=100&infId=OPEN17320190722180924242823&infSeq=1&downloadType=J&"
        + f"ATPT_OFCDC_SC_CODE={code}&SCHUL_NM=&MMEAL_SC_NM="
        + f"&MLSV_YMD={to_date.strftime('%Y%m%d')}&MLSV_YMD={end_date.strftime('%Y%m%d')}",
        "utf-8",
    )


with DAG(
    "meal_data_etl_dag",
    default_args={"retries": 2},
    # [END default_args]
    description="Korean school meal data ETL dag",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=[
        "schoolfit",
        "etl",
    ],
) as dag:
    dag.doc_md = __doc__

    def transform(code: str, **kwargs):
        ti = kwargs["ti"]
        extract_data_string = ti.xcom_pull(task_ids=f"extract_{code}_meal_data", key="return_value")
        meal_data: List[dict] = json.loads(extract_data_string)
        meal_data.pop(0)
        meal_data_json_string = json.dumps(meal_data)
        ti.xcom_push("meal_data", meal_data_json_string)

    def load(code: str, **kwargs):
        date = pendulum.now().add(days=7)
        ti = kwargs["ti"]
        meal_data_string = ti.xcom_pull(task_ids=f"transform_{code}_meal_data", key="meal_data")
        meal_data = json.loads(meal_data_string)

        hook = S3Hook(aws_conn_id="aws_default")
        hook.load_string(
            string_data=json.dumps(meal_data),
            key=f"{date.strftime('%Y%m%d')}/{code}.json",
            bucket_name=BUCKET_NAME,
            replace=True,
        )

    for code in ATPT_OFCDC_SC_CODES:
        after_weeks = pendulum.now().add(days=7)

        tasks = []
        for task_type in TASK_TYPES:
            if task_type == "extract":
                extract_task = SimpleHttpOperator(
                    task_id=f"extract_{code}_meal_data",
                    endpoint="portal/data/sheet/downloadSheetData.do",
                    method="POST",
                    data=get_meal_form_data(code, after_weeks, after_weeks),
                    response_check=lambda res: res.status_code == 200,
                    response_filter=lambda res: res.text,
                    http_conn_id="neis_openapi_http",
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
                        "Host": "open.neis.go.kr",
                        "Origin": "https://open.neis.go.kr",
                        "Referer": "https://open.neis.go.kr/portal/data/service/selectServicePage.do?page=1&rows=10&sortColumn=&sortDirection=&infId=OPEN17320190722180924242823&infSeq=1",
                    },
                )
                extract_task.doc_md = dedent(
                    f"""\
                    A Extract task to get korea school meal data ready for the rest of the data pipeline.
                    In this case, getting data is provided by NEIS OpenAPI.
                    This data is then put into xcom, so that it can be processed by the next task.
                    This task's regional code is {code}.
                    """
                )
                tasks.append(extract_task)

            elif task_type == "transform":
                transform_specified_code = partial(transform, code=code)
                transform_task = PythonOperator(
                    task_id=f"transform_{code}_meal_data",
                    python_callable=transform_specified_code,
                )
                transform_task.doc_md = dedent(
                    """\
                    #### Transform task
                    A meal data Transform task which takes in the collection of order data from xcom
                    and discards the first row, which is data that is not needed.
                    This value is then put into xcom, so that it can be processed by the next task.
                    """
                )
                tasks.append(transform_task)

            elif task_type == "load":
                load_specified_code = partial(load, code=code)
                load_task = PythonOperator(
                    task_id=f"load_{code}_meal_data",
                    python_callable=load_specified_code,
                )
                load_task.doc_md = dedent(
                    """\
                    #### Load task
                    A Load task which takes in the result of the Transform task, by reading it
                    from xcom and instead of saving it my s3 bucket.
                    """
                )
                tasks.append(load_task)

            if len(tasks) > 1:
                tasks[-2] >> tasks[-1]
