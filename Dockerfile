#TODO: Update

# Set base image
FROM python:3.8-slim

# Set working directory in the container
WORKDIR /code

# Copy the dependencies file to working directory
COPY requirements.txt .

# Install dependencies
RUN apt-get -y update
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the contents of the local directory to working directory
COPY src/ .
COPY ./bigQuery-SA.json bigQuery-SA.json
COPY ./spotify-client-id.txt spotify-client-id.txt
COPY ./spotify-client-secret.txt spotify-client-secret.txt

ENV PYTHONUNBUFFERED=1

# Command to run on container start
ENTRYPOINT ["python"]

CMD ["main.py"]