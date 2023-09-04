# AirFlow ETL/ELT пайплайн для сбора дашборда на базе данных Jivo

Материалы к статье ["Построение дашборда DataLens  для клиентского сервиса на базе Jivo, AirFlow и ClickHouse"]()

## Структура репозитория

- Папка `src` - ресурсы
    - `export_jivo_event.py` - скрипт сбора всех доступных данных по API Jivo.
    - Папка `dag` - ресурсы проекат для Airflow.
      - `jivo_event_updating.py` - DAG проекта.
      - Папка `sql` - SQL скрипиты AirFlow проекта.
        - `dds_jivo_event_dml.sql` - DML скрипт переноса данных из JSON в таблицу сырых данных.
        - `cdm_jivo_dashboard_truncate.sql` - DML скрипт очистки таблицы с витриной для дашборда.
        - `cdm_jivo_dashboard_dml.sql` - DML скрипт агрегации данных для витрины дашборда.
    - Папка `ddl` - SQL скрипты создания таблиц.
      - `dds_jivo_event_ddl.sql` - DDL скрипт создания таблицы для сырых данных.
      - `cdm_jivo_dashboard_ddl.sql` - DDL скрипт создания таблицы для витрины дашборда.

## Контакты

- **author**: Fatkhutdinov Ruslan
- **email**: workrf@gmail.com
- **tg**: @ruslan_fd
- **linkedin**: fathutdinov