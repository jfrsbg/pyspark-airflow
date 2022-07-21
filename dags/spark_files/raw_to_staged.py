from pyspark.sql.functions import lit


PATH_CONFIRMED = '/home/airflow/datalake/raw/covid19/time_series_covid19_confirmed_global.csv'
PATH_DEATHS = '/home/airflow/datalake/raw/covid19/time_series_covid19_deaths_global.csv'
PATH_RECOVERED = '/home/airflow/datalake/raw/covid19/time_series_covid19_recovered_global.csv'

OUTPUT_PATH = '/home/airflow/datalake/staged/covid19/'

class RawToStaged():
    def __init__(self, spark):
        self.spark = spark
        
    def union_datasets(self):
        '''Read all datasets, unify them with a label and deal with column names.
        No transformation are needed in this layer'''
        df_confirmed = self.spark.read.csv(PATH_CONFIRMED, header=True)
        df_deaths = self.spark.read.csv(PATH_DEATHS, header=True)
        df_recovered = self.spark.read.csv(PATH_RECOVERED, header=True)
        
        columns = [col.replace('/', '_').lower() for col in df_confirmed.columns]
        
        df_confirmed = df_confirmed.toDF(*columns)
        df_deaths = df_deaths.toDF(*columns)
        df_recovered = df_recovered.toDF(*columns)
        
        df_confirmed = df_confirmed.withColumn('dataset', lit('confirmed'))
        df_deaths = df_deaths.withColumn('dataset', lit('deaths'))
        df_recovered = df_recovered.withColumn('dataset', lit('recovered'))
        
        return df_confirmed.union(df_deaths).union(df_recovered)
    
    def execute(self):
        df = self.union_datasets()
        
        df.write.mode('overwrite')\
            .parquet(OUTPUT_PATH)