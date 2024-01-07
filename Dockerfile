FROM python:3.9-slim

# Install PostgreSQL development files
RUN apt-get update && apt-get install -y libpq-dev gcc

WORKDIR /usr/src/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Run the Python script when the container launches
CMD ["python", "./script.py"]
