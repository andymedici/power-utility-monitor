FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app.py .
COPY templates/ templates/

# Create data directory
RUN mkdir -p /app/data

# Environment variables
ENV DATABASE_PATH=/app/data/power_monitor.db
ENV DATA_DIR=/app/data
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8080

# Run application
CMD ["python", "app.py"]
