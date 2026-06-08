# Multi-platform load generator for parqtel
FROM python:3.11-slim

# Install curl for health checking if needed
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY src/main.py .

# Create results directory for CSV output
RUN mkdir /results && chmod 777 /results

# Run as non-root user
USER 65534

ENTRYPOINT ["python3", "main.py"]
