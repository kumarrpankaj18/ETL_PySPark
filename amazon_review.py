from pyspark.sql import *
from dotenv import load_dotenv
import os
import pyspark


load_dotenv()
file_path=os.getenv('data_path')

spark=SparkSession.builder\
     .appName("Amazon Review Data - Health_and_Personal_Care")\
     .master("local[*]")\
     .getOrCreate()

print("Spark session sucessfully created")

input_df=spark.read.json(file_path).show()
#input_df.createOrReplaceGlobalTempView('temptable')
#df=spark.sql("select * from global_temp.temptable")

input_df.select()