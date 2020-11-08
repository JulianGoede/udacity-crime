# use fix tag to ensure org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 is compatible with pyspark (version 3.0.1)
FROM jupyter/pyspark-notebook:712576e0e96b

COPY src/ /src/
COPY requirements.txt .

RUN pip3 install -r requirements.txt

ENV PYTHONPATH=/src

WORKDIR /src/crime/consumer

EXPOSE 4040
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1", "--master", "local[*]", "data_stream.py"]