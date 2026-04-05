# Lightweight Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Run the application using shell expansion for the PORT variable
CMD ["/bin/sh", "-c", "uvicorn src.main:app --host 0.0.0.0 --port 8040"]
