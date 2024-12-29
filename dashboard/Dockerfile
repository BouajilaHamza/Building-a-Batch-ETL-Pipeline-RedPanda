FROM python:3.12.7-slim

WORKDIR /app


COPY pyproject.toml pyproject.toml
RUN pip install poetry
RUN poetry install

COPY . .

CMD ["poetry", "run","streamlit", "run", "src/main.py", "--server.port", "8002"]
