FROM python:3.12.7-slim


WORKDIR /data_pipeline



RUN pip install poetry


COPY requirements.txt requirements.txt
COPY pyproject.toml pyproject.toml


RUN poetry install --no-root \
    && poetry run pip install -r requirements.txt


COPY . .

RUN chmod +x /data_pipeline/entrypoint.sh


EXPOSE 5000 8000
ENTRYPOINT [ "bash", "/data_pipeline/entrypoint.sh" ]
