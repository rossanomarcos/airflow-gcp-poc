from pyspark.sql import SQLContext, SparkSession

# Get spark session
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName("spark_job_dataproc") \
    .getOrCreate()

# spark
sc = spark.sparkContext
sqlContext = SQLContext(sc)
spark.conf.set('spark.sql.debug.maxToStringFields', 100)

def ios_parquet():
    raw_ios = ('gs://data-raw-zone/flight-data/')
    dst_new_fmt_ios = spark.read.option('enconding', 'UTF-8').option('multiLine', 'true').json(raw_ios)
    dst_new_fmt_ios.repartition(1).write.mode('append').mode('overwrite').parquet('gs://data-processed-zone/flight-data-fmt-parquet/')
    dst_ios = ('gs://data-processed-zone/flight-data-fmt-parquet/')
    df_ios = spark.read.parquet(dst_ios)
    df_ios.printSchema()

if __name__ == "__main__":
    ios_parquet()
