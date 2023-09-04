import requests
import json
from datetime import date, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator


API_TOKEN = Variable.get("jivo-token")
BASE_URL = Variable.get("jivo-url")

headers = {
    'Api-key': API_TOKEN
}


def jivo_events(**context):
    """ Получает данные по событиям Jivo из API и сохраняет в S3 бакет
    """
    ti = context["ti"]

    today = date.today()
    yesterday = date.today() - timedelta(days=1)

    response = requests.get(
        f'{BASE_URL}?limit=1000&start_time={yesterday}&end_time={today}',
        headers=headers)

    json_object = json.dumps(response.json()['webhooks'], ensure_ascii=False)
    with open('/opt/airflow/dags/posts.json', "w", encoding='utf8') as f:
        f.write(json_object)

    hook = S3Hook('s3-airflow')
    hook.load_file(
        filename='/opt/airflow/dags/posts.json',
        key=f'services_data/jivo/jivo_event_{yesterday}.json',        # Название и папка для сохраняемого файла с данными
        bucket_name='analitics-airflow')                              # Бакет, в который сохраняется файл

    ti.xcom_push(key='yesterday_date', value=str(yesterday))


def on_success_callback(context):
    """ Отправляет в телеграм сообщение об успешном завершении DAG
    """
    send_message = TelegramOperator(
        task_id='send_message_to_telegram',
        telegram_conn_id='telegram_good_bot',                         # Connection ID бота
        text='\U00002705 Формирование витрины для'
             ' <a href="https://datalens.yandex.ru/">дашборда с отчетом JIVO</a>' # Адрес на дашборд
             f" завершилось успешно в {context['ti'].end_date}"
    )
    return send_message.execute(context=context)


def on_failure_callback(context):
    """ Отправляет в телеграм сообщение об ошибке во время выполнения DAG
    """
    send_message = TelegramOperator(
        task_id='send_message_to_telegram',
        telegram_conn_id='tg_bad_bot',                                # Connection ID бота
        text='\U0000274C Формирование витрины для'
            ' <a href="https://datalens.yandex.ru/">дашборда с отчетом JIVO</a>' # Адрес на дашборд
             f" завершилось с ошибкой в таске {context['ti'].task_id}."
    )
    return send_message.execute(context=context)


default_args = {
    'owner': 'Ruslan Fatkhutdinov',                                   # Имя владельца DAG
    'email_on_failure': 'workrf@gmail.com',                           # Email владельца DAG
    'description': """ Собирает данные из Jivo в Clickhouse.
                       Агрегирует их для витрины дашборда
                   """
}


with DAG(dag_id='jivo_dashboard_updating',
            schedule_interval='0 2 * * *',
            start_date=days_ago(1),
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            tags=['dashboard', 'services'],
            catchup=False,
            default_args=default_args
    ) as dag:


    get_jivo_data = PythonOperator(
        task_id='get_jivo_data',
        python_callable=jivo_events,
        provide_context=True
    )

    dds_jivo_event_dml = ClickHouseOperator(
        task_id='dds_jivo_event_dml',
        clickhouse_conn_id='clickhouse_datago',                       # Connection ID Object Storage
        sql='sql/dds_jivo_event_dml.sql'
    )

    jivo_dashboard_truncate = ClickHouseOperator(
        task_id='jivo_dashboard_truncate',
        clickhouse_conn_id='clickhouse_datago',                       # Connection ID ClickHouse
        sql='sql/cdm_jivo_dashboard_truncate.sql'
    )

    jivo_dashboard_dml = ClickHouseOperator(
        task_id='jivo_dashboard_dml',
        clickhouse_conn_id='clickhouse_datago',                       # Connection ID ClickHouse
        sql='sql/cdm_jivo_dashboard_dml.sql'
    )


get_jivo_data >> dds_jivo_event_dml >> jivo_dashboard_truncate >> jivo_dashboard_dml

dag.doc_md = """ Собирает данные из Jivo в S3 бакет.
                 Агрегирует их для витрины дашборда
             """
