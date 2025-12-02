FROM python:3.10-slim

# Install system dependencies (required for psycopg2)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY python /app/python

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Default command (run pipeline)
CMD ["python3", "python/scripts/run_pipeline.py"]