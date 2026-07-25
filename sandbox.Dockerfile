# sandbox.Dockerfile — cap-sandbox: the untrusted-code boundary.
FROM python:3.12-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -m -u 1000 agent \
 && mkdir -p /workspace/output \
 && chown -R agent:agent /workspace
USER agent
WORKDIR /workspace
