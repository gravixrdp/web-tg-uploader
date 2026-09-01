FROM python:3.11-slim

# Install ffmpeg and required utilities for video processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    curl \
    libmagic1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency definition and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create runtime directories for SQLite persistence and ephemeral downloads
RUN mkdir -p /app/data /app/temp_downloads

# Copy application source code
COPY . .

# Set unbuffered output for real-time Railway logs
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
