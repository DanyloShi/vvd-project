from pyspark.sql.functions import monotonically_increasing_id, sha2, concat_ws, col, trim, explode, lit, struct, array, \
    lower, split
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql import SparkSession

#функція для запису у csv
import os
import shutil
from glob import glob

def save_single_csv(df, output_file):
    temp_dir = output_file + "_tmp"

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    if os.path.exists(output_file):
        os.remove(output_file)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_dir)

    part_file = glob(os.path.join(temp_dir, "part-*.csv"))[0]

    shutil.move(part_file, output_file)

    shutil.rmtree(temp_dir)

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

df = spark.read.csv(
    "data/raw/spotify_dataset.csv",
    header=True,
    schema=schema,
    multiLine=True,
    quote='"',
    escape='"',
    mode="PERMISSIVE"
)

df = df.withColumn(
    "song_id",
    sha2(concat_ws("||", col("Artist(s)"), col("song"), col("Album")), 256)
)

df = df.drop("text")
print("\nBasic song info")
songs_df = df.select(
    "song_id",
    "Artist(s)", "song", "Album"
)

songs_df.show(5, truncate=20)

print("\nRelease info")
release_info_df = df.select(
    "song_id",
    "Release Date", "Length", "emotion", "Popularity"
)

release_info_df.show(5, truncate=20)

print("\nAudio specifications")
df.select(
    "song_id",
    "Key", "Tempo", "Loudness (db)", "Time signature",
    "Energy", "Danceability", "Positiveness"
).show(5, truncate=20)

print("\nMore audio metrics")
df.select(
    "song_id",
    "Speechiness", "Liveness", "Acousticness", "Instrumentalness"
).show(5, truncate=20)

audio_features_df = df.select(
    "song_id",
    "Key",
    "Tempo",
    "Loudness (db)",
    "Time signature",
    "Energy",
    "Danceability",
    "Positiveness",
    "Speechiness",
    "Liveness",
    "Acousticness",
    "Instrumentalness"
)

print("\nWhat activities is it suitable for")
activity_df = df.select(
    "song_id",
    "Good for Party",
    "Good for Work/Study",
    "Good for Relaxation/Meditation",
    "Good for Exercise",
    "Good for Running",
    "Good for Yoga/Stretching",
    "Good for Driving"
)

activity_df.show(5, truncate=20)

print("\nSimilar tracks")
similar_songs_df = (
    df.select(
        "song_id",
        array(
            struct(
                lit(1).alias("similar_rank"),
                col("Similar Artist 1").alias("similar_artist"),
                col("Similar Song 1").alias("similar_song"),
                col("Similarity Score 1").alias("similarity_score")
            ),
            struct(
                lit(2).alias("similar_rank"),
                col("Similar Artist 2").alias("similar_artist"),
                col("Similar Song 2").alias("similar_song"),
                col("Similarity Score 2").alias("similarity_score")
            ),
            struct(
                lit(3).alias("similar_rank"),
                col("Similar Artist 3").alias("similar_artist"),
                col("Similar Song 3").alias("similar_song"),
                col("Similarity Score 3").alias("similarity_score")
            )
        ).alias("similarities")
    )
    .select("song_id", explode(col("similarities")).alias("sim"))
    .select(
        "song_id",
        col("sim.similar_rank"),
        trim(col("sim.similar_artist")).alias("similar_artist"),
        trim(col("sim.similar_song")).alias("similar_song"),
        col("sim.similarity_score")
    )
    .filter(col("similar_artist").isNotNull())
    .filter(col("similar_song").isNotNull())
    .filter(col("similar_artist") != "")
    .filter(col("similar_song") != "")
    .dropDuplicates()
)
similar_songs_df.show(5, truncate=20)

print("\nGenres")
song_genres_raw_df = (
    df.select(
        "song_id",
        explode(split(col("Genre"), ",")).alias("genre_name")
    )
    .withColumn("genre_name", trim(col("genre_name")))
    .filter(col("genre_name") != "")
    .filter(col("genre_name") != "Unknown")
    .dropDuplicates()
)

genres_df = (
    song_genres_raw_df
    .select("genre_name")
    .dropDuplicates()
    .withColumn(
        "genre_id",
        sha2(lower(trim(col("genre_name"))), 256)
    )
)

song_genres_df = (
    song_genres_raw_df
    .join(genres_df, "genre_name", "inner")
    .select("song_id", "genre_id")
    .dropDuplicates()
)
genres_df.show(5, truncate=20)
song_genres_df.show(5, truncate=20)

print("\nSchema DataFrame")
df.printSchema()