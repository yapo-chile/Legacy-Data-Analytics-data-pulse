from __future__ import print_function

import json

import requests
from datetime import timedelta, datetime

from lib.slack_msg import slack_msg_body

from airflow import models
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.models.variable import Variable
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.sensors.python import PythonSensor


sensor_config_gbq = json.loads(Variable.get("SENSOR_CONFIG_GBQ"))
SERVICE_SENSOR_URL = "https://py-scp-pipelines-healthchek-nasdocrtnq-ue.a.run.app"
sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")
connect_dockerhost = Variable.get('CONNECT_DOCKERHOST')
docker_image = Variable.get('DOCKER_IMAGE_DATA_PULSE_MASSIVE_LOAD')
SLACK_CONN_ID = 'slack'


# Calculate the date to be used for the next execution of the DAG
def get_date():
    start_date = "{{ dag_run.conf['start_date'] if dag_run.conf and 'start_date' in dag_run.conf else False}}"
    end_date = "{{ dag_run.conf['end_date'] if dag_run.conf and 'end_date' in dag_run.conf else False}}"
    if start_date and end_date:
        date = {"start_date": start_date, "end_date": end_date}
    else:
        execution_date = "{{ dag_run.logical_date }}"
        execution_date = execution_date - timedelta(days=1)
        execution_date = execution_date.date().strftime('%Y-%m-%d')
        date = {"start_date": execution_date, "end_date": execution_date}
    return date


def task_fail_slack_alert(context):
    slack_msg = slack_msg_body(
        context,
        riskiness="Medium",
        utility="This etl generates partners leads data in DWH.",
    )
    failed_alert = SlackWebhookOperator(
        task_id="failed_job_alert",
        http_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="airflow",
        dag=dag,
    )
    return failed_alert.execute(context=context)


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime(2022, 6, 16),
}


with models.DAG(
        'trigger_data_pulse_massive_load',
        tags=[
            'production',
            'ETL',
            'core',
            'git: legacy/data-pulse',
            'input: blocket',
            'input: gbq',
            'output: dwh'],
        schedule_interval=None,
        default_args=default_args,
        on_failure_callback=task_fail_slack_alert,
        max_active_runs=1
) as dag:

    def check_partition(**kwargs):
        test = requests.post(
            f"{SERVICE_SENSOR_URL}/{kwargs['table']}",
            json=kwargs['dates']
        )
        return (
            True if json.loads(test.content)["body"]["status_table"] == "OK" else False
        )

    run_massive_load = SSHOperator(
        task_id="run_massive_load",
        ssh_hook=sshHook,
        command=f"""{connect_dockerhost} <<EOF \n
        sudo docker pull {docker_image} \n
        sudo docker run -v /home/bnbiuser/secrets/pulse_auth:/app/pulse-secret \
                -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                -v /home/bnbiuser/secrets/blocket_db:/app/blocket-secret \
                -v /home/bnbiuser/secrets/bigquery_prod_gcp:/app/gbq-secret \
                -v /home/bnbiuser/secrets/pickles:/app/pickles \
                -e APP_PULSE_SECRET=/app/pulse-secret \
                -e APP_DB_SECRET=/app/blocket-secret \
                -e APP_DW_SECRET=/app/db-secret \
                -e APP_GBQ_SECRET=/app/gbq-secret \
                -e APP_PICKLES=/app/pickles \
                {docker_image} \
                -date_from={get_date()['start_date']} \
                -date_to={get_date()['end_date']}"""
    )

    for table in ["gbq_staging_leads"]:
        check_table_partition_exists = PythonSensor(
            task_id="sensor_partition_exists_{}".format(table),
            poke_interval=sensor_config_gbq["poke_interval"],
            mode="reschedule",
            timeout=sensor_config_gbq["timeout"],
            python_callable=check_partition,
            op_kwargs={"table": table, "dates": get_date()},
        )

        check_table_partition_exists >> run_massive_load