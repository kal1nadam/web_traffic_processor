# docker/Dockerfile
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

ENV POETRY_VERSION=1.8.3
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

# 1) Copy only dependency files and install deps (no project yet)
COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.create false \
 && poetry install --no-interaction --no-ansi --no-root

# 2) Now copy your source and install the project itself
COPY src ./src
# (Optional) add tests, scripts, etc, as needed:
# COPY tests ./tests

RUN poetry install --no-interaction --no-ansi

ENTRYPOINT ["python", "-m", "app.cli"]
