import os

from pyspark.sql import SparkSession


def open_spark_connect_session():
    session = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()

    return session
