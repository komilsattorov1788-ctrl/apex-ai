FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies needed for compiling python packages (like asyncpg)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements file first to cache the pip install step
COPY requirements.txt .

# Upgrade pip securely and install dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the entire project so static assets and backend are available
COPY . /app/

# Expose standard port. Railway will provide its own $PORT variable at runtime
ENV PORT=8000
EXPOSE 8000

# Move into backend to run the startup script, allowing main.py to serve frontend files from ../
WORKDIR /app/backend

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
