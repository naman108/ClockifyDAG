from __future__ import annotations

# [START tutorial]
# [START import_module]
import json
from airflow import DAG

import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# [END import_module]

POSTGRES_CONN_ID = "Life-DB-Postgres-Con"
WORKSPACE_ID = "6085d94933c9f53b5dd2b895"
USER_ID = "6085d94833c9f53b5dd2b892"
TIME_ENTRY_URL = f"api/v1/workspaces/{WORKSPACE_ID}/user/{USER_ID}/time-entries"
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

# [START instantiate_dag]
@dag(dag_id="clockify_dag", schedule=None, start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), catchup=False, tags=["clockify"])
def clockify_dag():
    @task(task_id="task_transform_clockify", show_return_value_in_logs=True)
    def task_transform_clockify(clockify_entries:list) -> list:

        clockify_entries_list = clockify_entries
        
        for clockify_entry in clockify_entries_list:
            clockify_entry['duration'] = clockify_entry['timeInterval']['duration']
            clockify_entry['end'] = clockify_entry['timeInterval']['end']
            clockify_entry['start'] = clockify_entry['timeInterval']['start']
            clockify_entry.pop('timeInterval')
        return clockify_entries

    @task(task_id="task_build_clockify_insert", show_return_value_in_logs=True)
    def task_build_clockify_insert(clockify_entry_list: list) -> str:
        base_sql = """
        INSERT INTO public."time-records" (id, user_id, workspace_id, project_id, task_id, description, billable, is_locked, type, start_time, end_time)
        VALUES
        """
        insert_sql = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
        for clockify_entry in clockify_entry_list:
            values = insert_sql % (
                clockify_entry['id'],
                clockify_entry['userId'],
                clockify_entry['workspaceId'],
                clockify_entry['projectId'],
                clockify_entry['taskId'],
                clockify_entry['description'],
                clockify_entry['billable'],
                clockify_entry['isLocked'],
                #clockify_entry['tagIds'],
                clockify_entry['type'],
                clockify_entry['start'],
                clockify_entry['end'],
                #clockify_entry['duration']
            )
            base_sql += values + ","
        base_sq = base_sql[:-1] + """
ON CONFLICT (id) DO UPDATE SET
    user_id = EXCLUDED.user_id,
    workspace_id = EXCLUDED.workspace_id,
    project_id = EXCLUDED.project_id,
    task_id = EXCLUDED.task_id,
    description = EXCLUDED.description,
    billable = EXCLUDED.billable,
    is_locked = EXCLUDED.is_locked,
    type = EXCLUDED.type,
    start_time = EXCLUDED.start_time,
    end_time = EXCLUDED.end_time;
"""

        return base_sq
    
    @task(task_id="done", trigger_rule="all_done")
    def done(save_result):
        print("Done")

    task_extract_clockify = HttpOperator(
        task_id="task_extract_clockify",
        method="GET",
        http_conn_id="clockify_api",
        endpoint=TIME_ENTRY_URL,
        data={"start": (datetime.now()-timedelta(weeks=1, days=1)).strftime(DATETIME_FMT), "end": (datetime.now()-timedelta(days=1)).strftime(DATETIME_FMT)},
        response_filter=lambda response: json.loads(response.text)
    )

    transform = task_transform_clockify(task_extract_clockify.output)

    insert_sql = task_build_clockify_insert(transform)

    task_save_clockify = SQLExecuteQueryOperator(  
            task_id="task_save_clockify",
            conn_id=POSTGRES_CONN_ID,
            sql=insert_sql,
            autocommit=True,
        )

    done_out = done(task_save_clockify.output)


clockify_dag()