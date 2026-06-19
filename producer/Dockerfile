FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer script
COPY produce_sales_events.py .

# Run producer
CMD ["python", "-u", "produce_sales_events.py"]
