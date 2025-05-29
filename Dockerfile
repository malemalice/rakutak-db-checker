FROM python:3.8-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    default-libmysqlclient-dev \
    pkg-config \
    python3-dev \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create logs directory
RUN mkdir -p logs

# Add current directory to PYTHONPATH
ENV PYTHONPATH=/app

# Make entrypoint script executable and ensure it has Unix line endings
RUN chmod +x entrypoint.sh && \
    sed -i 's/\r$//' entrypoint.sh 