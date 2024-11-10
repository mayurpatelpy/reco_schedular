# Use an official Python runtime as a parent image
FROM python:3.10-buster

# Set the working directory in the container
WORKDIR /app
RUN apt-get update && apt-get install -y git

COPY .netrc /root/.netrc
# Copy the requirements file to the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI code to the container
COPY . .

# Expose the port that FastAPI runs on (default 8000)


# Command to run the FastAPI app using uvicorn
#CMD ["celery", "-A", "celeryapp", "worker", "--loglevel=info"]
