FROM python:3.11-slim

# Install ffmpeg, aria2 multi-connection downloader, and utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    aria2 \
    ca-certificates \
    curl \
    libmagic1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Invalidate cache for new application version
ENV APP_VERSION=2.0.0
ENV PYTHONUNBUFFERED=1

# Copy dependency definition and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create runtime directories for SQLite persistence and ephemeral downloads
RUN mkdir -p /app/data /app/temp_downloads /app/static

# Copy all application source code, static assets, and modules
COPY . .

CMD ["python", "main.py"]
