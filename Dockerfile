FROM python:3.10-slim

WORKDIR /app

# Enable bytecode compilation, better performance
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./backend /app/backend
WORKDIR /app/backend

# Use Gunicorn as a process manager with Uvicorn workers for optimal performance on a single pod before K8s handles horizontal scaling
CMD ["gunicorn", "main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
