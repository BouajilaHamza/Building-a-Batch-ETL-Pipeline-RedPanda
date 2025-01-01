poetry run airflow db init
poetry run airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin --password admin

poetry run airflow scheduler &
poetry run airflow webserver --port 5000 &
poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000 &
poetry run python src/services/etl/redpanda_transformer.py
