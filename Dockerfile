# Use the official Python image
FROM python:3.11.11

# Set the working directory inside the container
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy all source files except those in .dockerignore
COPY . .

# Ensure logs are written immediately (useful for debugging)
ENV PYTHONUNBUFFERED=1

# Run the trading bot script
CMD ["python", "main_day_trader.py"]
