from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("TestSpark").getOrCreate()

data = [("Moonlight", 2), ("Diamonds", 3), ("Cold Treats", 2.5)]

data = [(song, None if dur is None else float(dur)) for song, dur in data]

schema = StructType([
    StructField("Song", StringType(), True),
    StructField("Duration", DoubleType(), True),
])

df = spark.createDataFrame(data, schema=schema)
df.show()