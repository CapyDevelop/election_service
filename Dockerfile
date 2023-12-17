FROM python:3.11-slim

RUN apt-get update && apt-get install -y git

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .
COPY .env_docker .env

CMD ["python", "main.py"]