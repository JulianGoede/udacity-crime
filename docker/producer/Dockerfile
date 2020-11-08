ARG TAG=3.7.9
FROM python:$TAG

COPY src/ /src/
COPY requirements.txt .

RUN pip install -r requirements.txt

ENV PYTHONPATH=/src

WORKDIR /src/crime/producer

CMD ["python3", "kafka_server.py"]