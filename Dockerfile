# Base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app code
COPY . .

# Expose the port on which the app will run
EXPOSE 5000

# Command to run the Flask app
CMD ["python3", "server.py"]