# airflow_etl_procces

## Использование

1. Клонируйте этот репозиторий на свой локальный компьютер.
2. Используйте команду docker-compose up -d, находясь в директории с файлом yml.
3. Настройте коннекшен в Airflow для оператора postgreshook - id_connection = abdullaev_conn или задать свой для postgres_conn_id
4. Настроить конфиг для дага, если необходимо
5. Подключиться к базе postgres
6. Подключить базу postgres в superset
