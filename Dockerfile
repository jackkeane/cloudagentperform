# Dockerfile — platform image shared by api and worker
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY core/ core/
COPY sandbox/ sandbox/
COPY agent/ agent/
COPY worker/ worker/
COPY api/ api/
COPY cli/ cli/
COPY fixtures/ fixtures/
RUN pip install --no-cache-dir .
