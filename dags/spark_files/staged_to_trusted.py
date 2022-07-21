from pyspark.sql.functions import col, lit, when, to_timestamp, year, month

STAGED_PATH = '/home/airflow/datalake/staged/covid19/'

OUTPUT_PATH = '/home/airflow/datalake/trusted/covid19/'

class StagedToTrusted():
    def __init__(self, spark):
        self.spark = spark
        
    def stack_multiple(self, cols=None,output_columns=["col","values"]):
        '''Creates a string for the stack function wich will be used in selectExpr'''
        cols= [cols] if isinstance(cols,str) else cols

        return f"""stack({len(cols)},{','.join(map(','.join,
                   (zip([f'"{i}"' for i in cols],[f"{i}" for i in cols]))))}) 
                    as ({','.join(output_columns)})"""
    
    def transform(self, df):
        '''Unpivot all date columns and transform them all'''
        
        df = df.selectExpr('province_state', 'country_region', 'lat', 'long', 'dataset', self.stack_multiple(df.columns[4:-1], ['data', 'qtde']))
        
        return df.withColumn('quantidade_confirmados', when(col('dataset') == 'confirmed', col('qtde')).otherwise(lit(0)).cast('long'))\
            .withColumn('quantidade_mortes', when(col('dataset') == 'deaths', col('qtde')).otherwise(lit(0)).cast('long'))\
            .withColumn('quantidade_recuperados', when(col('dataset') == 'recovered', col('qtde')).otherwise(lit(0)).cast('long'))\
            .withColumn('data', to_timestamp('data', 'M_d_yy'))\
            .withColumn('ano', year('data'))\
            .withColumn('mes', month('data'))\
            .withColumn('latitude', col('lat').cast('double'))\
            .withColumn('longitude', col('long').cast('double'))\
            .withColumnRenamed('province_state', 'estado')\
            .withColumnRenamed('country_region', 'pais')\
            .drop('dataset', 'qtde', 'lat', 'long')
    
    def execute(self):
        '''Entrypoint to all functions'''
        df = self.spark.read.parquet(STAGED_PATH)
        df = self.transform(df)
        
        df.coalesce(1).write.mode('overwrite')\
            .partitionBy("ano", "mes")\
            .parquet(OUTPUT_PATH)