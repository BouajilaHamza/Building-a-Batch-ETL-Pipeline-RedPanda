FROM python:3.12.7-slim

WORKDIR /app

COPY pyproject.toml pyproject.toml
RUN pip install poetry \
    && poetry install

COPY . .

CMD ["poetry", "run", "python", "main.py"]
