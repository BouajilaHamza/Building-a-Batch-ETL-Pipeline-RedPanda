airflow db init
airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin --password admin

airflow scheduler &
airflow webserver --port 5000 &
poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000
