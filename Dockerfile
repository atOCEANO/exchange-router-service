FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd --create-home --uid 1000 appuser && chown -R appuser /app
USER appuser

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8040", "--ws-ping-interval", "30", "--ws-ping-timeout", "60"]
