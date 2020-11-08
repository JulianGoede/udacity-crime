import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from crime.producer.kafka_server import BROADCAST_URL, TOPIC
import pyspark.sql.functions as psf

schema = StructType([
    StructField('crime_id', dataType=StringType(), nullable=True),
    StructField('original_crime_type_name', dataType=StringType(), nullable=True),
    StructField('report_date', dataType=TimestampType(), nullable=True),
    StructField('call_date', dataType=TimestampType(), nullable=True),
    StructField('offense_date', dataType=TimestampType(), nullable=True),
    StructField('call_time', dataType=StringType(), nullable=True),
    StructField('call_date_time', dataType=TimestampType(), nullable=True),
    StructField('disposition', dataType=StringType(), nullable=True),
    StructField('address', dataType=StringType(), nullable=True),
    StructField('city', dataType=StringType(), nullable=True),
    StructField('state', dataType=StringType(), nullable=True),
    StructField('agency_id', dataType=StringType(), nullable=True),
    StructField('address_type', dataType=StringType(), nullable=True),
    StructField('common_location', dataType=StringType(), nullable=True),
])

DATA_DIR = os.getenv("DATA_DIR", f'{os.path.abspath(os.path.dirname(__file__))}/data')
RADIO_CODE_JSON = f'{DATA_DIR}/radio_code.json'

RADIO_JSON_SCHEMA = StructType([
    StructField('disposition_code', dataType=StringType(), nullable=False),
    StructField('description', dataType=StringType(), nullable=False)
])


def run_spark_job(spark: SparkSession,
                  topic: str = TOPIC,
                  bootstrap_servers: str = BROADCAST_URL,
                  schema: StructType = schema
                  ):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("maxOffsetsPerTrigger", "200") \
        .option("rowsPerSecond", "5") \
        .option("stopGracefullyOnShutdown", "true") \
        .option("spark.default.parallelism", "2") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df.select(psf.from_json(psf.col('value'), schema).alias("DF")).select("DF.*")

    distinct_table = service_table.select("original_crime_type_name", "disposition", "call_date_time").distinct() \
        .withWatermark("call_date_time", "60 minutes")

    # count the number of original crime type
    # query = distinct_table \
    #     .groupBy(psf.window("call_date_time", "60 minutes", "10 minutes"), "original_crime_type_name") \
    #     .count() \
    #     .writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()
    #
    # query.awaitTermination()

    radio_code_df = spark.read.option("multiline", "true").json(RADIO_CODE_JSON, schema=RADIO_JSON_SCHEMA)

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = distinct_table \
        .groupBy(psf.window("call_date_time", "60 minutes", "10 minutes"), "original_crime_type_name", "disposition") \
        .count() \
        .join(radio_code_df, "disposition", "left_outer")

    join_query = join_query \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    join_query.awaitTermination()


def read_json(path):
    import json
    with open(path) as f:
        return json.load(f)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 4040) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark started")
    try:
        run_spark_job(spark)
        logger.info("spark job finished successfully")
    except Exception as e:
        logger.error("spark job failed with exception", exc_info=True)
    finally:
        logger.info("shutdown spark")
        spark.stop()
