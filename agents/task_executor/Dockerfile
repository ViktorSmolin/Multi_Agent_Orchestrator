FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY agents/task_executor/requirements.txt agents/task_executor/requirements.lock ./
COPY agents/task_executor/vendor/wheels ./vendor/wheels
COPY agents/task_executor/vendor/wheels-arm64 ./vendor/wheels

RUN pip install --no-index --find-links=/app/vendor/wheels --require-hashes -r requirements.lock

COPY agents/task_executor/agent.py agents/task_executor/protocol.py agents/task_executor/healthcheck.py agents/task_executor/trigger_task.py agents/task_executor/state.py ./
COPY agents/task_executor/tests ./tests
COPY examples/python/event_example.py ./event_example.py

RUN useradd --create-home --uid 10001 agent \
    && mkdir -p /app/state \
    && chown agent:agent /app/state

USER agent

EXPOSE 8091

CMD ["python", "-u", "agent.py"]
