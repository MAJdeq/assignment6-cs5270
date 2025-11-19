# Use official Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy application code and requirements
COPY consumer.py .
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set default environment variables (can be overridden at runtime)
ENV AWS_REGION=us-east-1
ENV QUEUE_NAME=cs5270-requests

# Use shell form CMD so environment variables are expanded
CMD ["sh", "-c", "python consumer.py --storage queue --queue \"$QUEUE_NAME\""]
