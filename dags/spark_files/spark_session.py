from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("local[*]") \
        .appName("airflow_app") \
        .config('spark.executor.memory', '6g') \
        .config('spark.driver.memory', '6g') \
        .config("spark.driver.maxResultSize", "1048MB") \
        .config("spark.port.maxRetries", "100") \
        .getOrCreate()