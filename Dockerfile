# backend/Dockerfile
FROM python:3.11-slim AS builder

# Build stage
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements-optimized.txt .

# Install PyTorch CPU first with correct index
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch

# Now install other dependencies
RUN pip install --no-cache-dir -r requirements-optimized.txt

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Install only runtime dependencies
RUN apt-get update && apt-get install -y \
    libmagic1 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy installed packages from builder
COPY --from=builder usr/local /usr/local

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]