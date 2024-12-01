FROM python:3.12.7-slim

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN pip install poetry
RUN poetry install

COPY . .

CMD ["poetry", "run", "python", "main.py"]
