import ast
import csv
import json
import logging
import os
import random
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from faker import Faker

logger = logging.getLogger(__name__)

default_args = {
    "owner": "abdullaevr",
    "retries": 1,
}

class ParseOsagoPolicy:
    def get_policy(self, json_df: pd.DataFrame):
        import numpy as np
        processed_data = json_df["osago_policy"].apply(self._parse_json)
        processed_data = pd.json_normalize(processed_data)
        processed_data.fillna(np.nan, inplace=True)
        columns_new = list(processed_data.columns)
        json_df[columns_new] = processed_data[columns_new]

        processed_data_flat = json_df[
            [
                "id",
                "fraud",
                "status",
            ]
            + columns_new
        ].copy()

        return processed_data_flat
    @staticmethod
    def _parse_json(val):
        try:
            return json.loads(val)["osago_policy"]
        except:
            return json.loads(
                json.dumps(
                    ast.literal_eval(val.replace("null", "None")),
                    ensure_ascii=False,
                )
                .encode("utf8")
                .decode()
            )
        
parser = ParseOsagoPolicy()


with DAG(
    dag_id="create_and_push_db_df",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 12, 10),
    catchup=False,
    tags=["seminar","df_create", "df_push"],
) as dag:
    # Генерации данных с помощью faker
    def generate_faker_data(**kwargs):
        try:
            fake = Faker()
            rows = [
                {"id": i,
                 "osago_policy": 
                 {
                    "first_name": fake.first_name(), 
                    "last_name": fake.last_name(),
                    "email": fake.email(), 
                    "phone_number": fake.phone_number(), 
                    "address": fake.address()
                },
                "status": "fully_done",
                "fraud": random.choice(["true", "false"])
                } for i in range(1, 101)
            ]
            
            base_dir = "/opt/airflow/dags/tmp"
            file_name = "data.csv"
            file_path = os.path.join(base_dir, file_name)

            with open(file_path, "w") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

            # передаем путь файла через XCom
            kwargs["ti"].xcom_push(key="file_path", value=file_path)

        except Exception as e:
            logger.error(f"Ошибка в генерации и сохранении даных {e}")
        
    generate_faker_data_task = PythonOperator(
        task_id="generate_faker_data",
        python_callable=generate_faker_data,
        op_kwargs={"file_path": "dags/tmp/data.csv"}
    )

    move_file_to_patch_task = BashOperator(
        task_id="move_file_to_patch",
        bash_command="mv {{ ti.xcom_pull(task_ids='generate_faker_data', key='file_path') }} /opt/airflow/dags/tmp/processed_data/"
    )

    # Загрузка данных в Postgres
    def load_data_to_postgres(**kwargs):
        try:
            file_path = kwargs["ti"].xcom_pull(key="file_path", task_ids="move_file_to_patch_task")
            file_path = "/opt/airflow/dags/tmp/processed_data/data.csv"

            if not file_path:
                raise logger.error("Путь к файлу не найден")
            data = pd.read_csv(file_path)

            logger.info("Инициализация PostgresHook")
            pg_hook = PostgresHook(postgres_conn_id="abdullaev_conn")

            table_name = "stg_policy_raw"
            schema_name = "stg_osago"

            sql_create_table = f"""
                CREATE SCHEMA IF NOT EXISTS {schema_name};
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    email VARCHAR(100),
                    phone_number VARCHAR(20),
                    address TEXT
                );
            """
            logger.info(f"Создание схемы {schema_name}")
            pg_hook.run(sql_create_table)
            
            logger.info(f"Создание таблицы {schema_name}.{table_name}")
            pg_hook.run(sql_create_table)

            data.to_sql(table_name, 
                pg_hook.get_sqlalchemy_engine(), 
                schema = schema_name,
                if_exists="replace", 
                index=False)

            # Подсчет количества записей
            kwargs['ti'].xcom_push(key="count_row", value=data.shape[0])
            # передаем название схемы через XCom
            kwargs["ti"].xcom_push(key="schema_name", value=schema_name)
            # передаем название таблицы через XCom
            kwargs["ti"].xcom_push(key="table_name", value=table_name)
        except Exception as e:
            logger.error(f"Ошибка на этапе переноса и добавление данных в БД {e}")
    
    load_data_to_postgres_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_data_to_postgres
    )
    # Вывод количества записей
    def log_count_rows(**kwargs):
        row_count = kwargs['ti'].xcom_pull(task_ids='load_data_to_postgres', key='count_row')
        logger.info(f"Количество загруженных записей: {row_count}")
    
    log_count_rows_task = PythonOperator(
        task_id="log_record_count",
        python_callable=log_count_rows
    )

    def process_data(**kwargs):
        try:
            logger.info("Инициализация PostgresHook")
            pg_hook = PostgresHook(postgres_conn_id="abdullaev_conn")
            engine = pg_hook.get_sqlalchemy_engine()

            schema_raw_name = kwargs['ti'].xcom_pull(task_ids='load_data_to_postgres', key='schema_name')
            raw_table_name = kwargs['ti'].xcom_pull(task_ids='load_data_to_postgres', key='table_name')

            schema_name_dim = "dim_osago"
            table_name_dim = "dim_policy_preproc"

            raw_data = pd.read_sql(f"SELECT * FROM {schema_raw_name}.{raw_table_name}", engine)

            sql_create_table = f"""
                CREATE SCHEMA IF NOT EXISTS {schema_name_dim};
                CREATE TABLE IF NOT EXISTS {schema_name_dim}.{table_name_dim} (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    email VARCHAR(100),
                    phone_number VARCHAR(20),
                    address TEXT
                );
            """
            logger.info(f"Создание схемы {schema_name_dim}")
            pg_hook.run(sql_create_table)

            logger.info(f"Создание таблицы {schema_name_dim}.{table_name_dim}")
            pg_hook.run(sql_create_table)
            
            processed_data_flat = parser.get_policy(raw_data)

            processed_data_flat.to_sql(table_name_dim, 
                                       engine, 
                                       schema=schema_name_dim, 
                                       if_exists="replace", 
                                       index=False)

        except Exception as e:
            logger.error(f"Ошибка обработки данных: {e}")

    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )



    generate_faker_data_task >> move_file_to_patch_task >> load_data_to_postgres_task >> log_count_rows_task >> process_data_task
    