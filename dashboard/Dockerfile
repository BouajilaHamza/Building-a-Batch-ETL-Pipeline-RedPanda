FROM python:3.12.7-slim

WORKDIR /app


COPY pyproject.toml pyproject.toml
RUN pip install poetry
RUN poetry install

COPY . .

CMD ["poetry", "run","uvicorn", "src.main:app"]
