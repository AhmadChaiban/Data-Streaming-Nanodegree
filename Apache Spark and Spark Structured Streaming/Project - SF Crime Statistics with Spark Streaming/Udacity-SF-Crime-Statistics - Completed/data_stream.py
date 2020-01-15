import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

## Json file details
#         "crime_id": "183653763",
#         "original_crime_type_name": "Traffic Stop",
#         "report_date": "2018-12-31T00:00:00.000",
#         "call_date": "2018-12-31T00:00:00.000",
#         "offense_date": "2018-12-31T00:00:00.000",
#         "call_time": "23:57",
#         "call_date_time": "2018-12-31T23:57:00.000",
#         "disposition": "ADM",
#         "address": "Geary Bl/divisadero St",
#         "city": "San Francisco",
#         "state": "CA",
#         "agency_id": "1",
#         "address_type": "Intersection",
#         "common_location": ""

# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", IntegerType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date",TimestampType(),True),
    StructField("call_date",TimestampType(),True),
    StructField("call_time",TimestampType(),True),
    StructField("call_date_time",TimestampType(),True),
    StructField("disposition",StringType(),True),
    StructField("address",StringType(),True),
    StructField("city",StringType(),True),
    StructField("state",StringType(),True),
    StructField("agency_id",IntegerType(),True),
    StructField("address_type",StringType(),True),
    StructField("common_location",StringType(),True)
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:58050") \
        .option("subscribe", "com.udacity.sf.crime") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df \
        .selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select("call_date_time","original_crime_type_name","disposition")

    # count the number of original crime type
    agg_df = distinct_table.select("*").withWatermark("call_date_time","3600 seconds").groupby("call_date_time","original_crime_type_name").count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .format("console") \
        .option("truncate", "false") \
        .start()
            

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(query.disposition == radio_code_df.disposition,how=="right")


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port",3000) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
