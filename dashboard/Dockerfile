FROM python:3.12.7-slim

WORKDIR /dashboard

RUN pip install poetry

COPY pyproject.toml pyproject.toml
RUN poetry install

COPY . .
ENV PYTHONPATH=/
CMD ["poetry", "run","streamlit", "run", "src/main.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
