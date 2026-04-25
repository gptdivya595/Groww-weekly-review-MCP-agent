FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN python -m pip install --no-cache-dir uv

COPY . /app

RUN uv pip install --system -e .

CMD ["pulse", "--help"]
