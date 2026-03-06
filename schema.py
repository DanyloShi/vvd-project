from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql import SparkSession

schema = StructType([
    StructField("Artist(s)", StringType(), True),
    StructField("song", StringType(), True),
    StructField("text", StringType(), True),
    StructField("Length", StringType(), True),
    StructField("emotion", StringType(), True),
    StructField("Genre", StringType(), True),
    StructField("Album", StringType(), True),
    StructField("Release Date", StringType(), True),
    StructField("Key", StringType(), True),
    StructField("Tempo", FloatType(), True),
    StructField("Loudness (db)", StringType(), True),
    StructField("Time signature", StringType(), True),
    StructField("Explicit", StringType(), True),
    StructField("Popularity", IntegerType(), True),
    StructField("Energy", FloatType(), True),
    StructField("Danceability", FloatType(), True),
    StructField("Positiveness", FloatType(), True),
    StructField("Speechiness", FloatType(), True),
    StructField("Liveness", FloatType(), True),
    StructField("Acousticness", FloatType(), True),
    StructField("Instrumentalness", FloatType(), True),
    StructField("Good for Party", IntegerType(), True),
    StructField("Good for Work/Study", IntegerType(), True),
    StructField("Good for Relaxation/Meditation", IntegerType(), True),
    StructField("Good for Exercise", IntegerType(), True),
    StructField("Good for Running", IntegerType(), True),
    StructField("Good for Yoga/Stretching", IntegerType(), True),
    StructField("Good for Driving", IntegerType(), True),
    StructField("Good for Social Gatherings", IntegerType(), True),
    StructField("Good for Morning Routine", IntegerType(), True),
    StructField("Similar Artist 1", StringType(), True),
    StructField("Similar Song 1", StringType(), True),
    StructField("Similarity Score 1", FloatType(), True),
    StructField("Similar Artist 2", StringType(), True),
    StructField("Similar Song 2", StringType(), True),
    StructField("Similarity Score 2", FloatType(), True),
    StructField("Similar Artist 3", StringType(), True),
    StructField("Similar Song 3", StringType(), True),
    StructField("Similarity Score 3", FloatType(), True),
])

spark = SparkSession.builder.appName("MusicData").getOrCreate()

df = spark.read.csv("spotify_dataset.csv", header=True, schema=schema)

print("\nBasic song info")
df.select(
    "Artist(s)", "song", "Genre", "Album"
).show(5, truncate=20)

print("\nRelease info")
df.select(
    "Release Date", "Length", "emotion", "Popularity"
).show(5, truncate=20)

print("\nAudio specifications")
df.select(
    "Key", "Tempo", "Loudness (db)", "Time signature",
    "Energy", "Danceability", "Positiveness"
).show(5, truncate=20)

print("\nMore audio metrics")
df.select(
    "Speechiness", "Liveness", "Acousticness", "Instrumentalness"
).show(5, truncate=20)

print("\nWhat activities is it suitable for")
df.select(
    "Good for Party",
    "Good for Work/Study",
    "Good for Relaxation/Meditation",
    "Good for Exercise",
    "Good for Running",
    "Good for Yoga/Stretching",
    "Good for Driving"
).show(5)

print("\nSimilar tracks")
df.select(
    "Similar Artist 1", "Similar Song 1", "Similarity Score 1",
    "Similar Artist 2", "Similar Song 2", "Similarity Score 2",
    "Similar Artist 3", "Similar Song 3", "Similarity Score 3"
).show(5, truncate=20)

print("\nSchema DataFrame")
df.printSchema()