FROM python:3.10-slim

# Install system dependencies (required for psycopg2 and some pandas engines)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements if they exist, otherwise install manually for speed
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || \
    pip install --no-cache-dir pandas psycopg2-binary openpyxl pyarrow tabulate

# Copy the rest of the application
COPY . .

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Default command
CMD ["python3", "python/scripts/run_pipeline.py"]