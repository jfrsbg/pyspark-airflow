from pyspark.sql import Window
from pyspark.sql.functions import col, avg

TRUSTED_PATH = '/home/airflow/datalake/trusted/covid19/'
OUTPUT_PATH = '/home/airflow/datalake/refined/covid19/'

class TrustedToRefined():
    def __init__(self, spark):
        self.spark = spark
        
    
    def transform(self, df):
        '''Transform columns calculating rolling average, partitioning by country to increase spark performance'''
        days = lambda i: i * 86400
        
        windowSpec = Window.partitionBy('pais').orderBy(col("data").cast('long')).rangeBetween(-days(6), 0)
        
        df = df.withColumn('media_movel_confirmados', avg("quantidade_confirmados").over(windowSpec).cast('long'))\
            .withColumn('media_movel_mortes', avg("quantidade_mortes").over(windowSpec).cast('long'))\
            .withColumn('media_movel_recuperados', avg("quantidade_recuperados").over(windowSpec).cast('long'))\
            .select('pais', 'data', 'media_movel_confirmados', 'media_movel_mortes', 'media_movel_recuperados', 'ano')
     
        return df
    
    def execute(self):
        '''Entrypoint to all functions'''
        df = self.spark.read.parquet(TRUSTED_PATH)
        
        df = self.transform(df)
        
        df.coalesce(1).write.mode('overwrite')\
            .partitionBy("ano")\
            .parquet(OUTPUT_PATH)