from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("DirtyData").getOrCreate()


dirty_data = [
    ("123-abc", "USER1", "yes", "2021-13-01", "5", "  great product  ", None, "n/a", "", {"extra": "value"}),
    ("123-abc", "USER1", "yes", "2021-13-01", "5", "  great product  ", None, "n/a", "", {"extra": "value"}),  # Duplicate
    ("", None, None, "not-a-date", "bad", "", "USA", "", "null", None),
    (None, "user2", "Y", "2020/12/01", 4.0, "I liked it", "Canada", "unknown", "abc", None),
    ("456-def", "UsEr2", "n", "12-01-2020", "3.5", "Not bad.", "", "??", None, None),
    ("789-ghi", "user3", "No", "2022-05-20", 2, "Terrible!", "UK", "uk", "0", {}),
    ("000-xyz", "user4", "maybe", "2021-11-11", 5, "Best ever!!", "India", "n/a", "NA", {"invalid": 999}),
    ("999-ooo", "user5", "", None, "", None, None, None, None, None)
]

schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("verified_purchase", StringType(), True),
    StructField("purchase_date", StringType(), True),
    StructField("rating", StringType(), True),  # intentionally string
    StructField("review", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

df = spark.createDataFrame(dirty_data, schema)
df.show(truncate=False)
df.printSchema()


output_path = "C:\\Users\\kumaa\\Downloads\\dirty_dataset.json"
df.write.mode("overwrite").json(output_path)
print('Filed saved successfully')



