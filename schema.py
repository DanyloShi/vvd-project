from pyspark.sql.functions import monotonically_increasing_id
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

df = df.withColumn("song_id", monotonically_increasing_id())

df = df.drop("text")
print("\nBasic song info")
songs_df = df.select(
    "song_id",
    "Artist(s)", "song", "Genre", "Album"
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
similar_songs_df = df.select(
    "song_id",
    "Similar Artist 1", "Similar Song 1", "Similarity Score 1",
    "Similar Artist 2", "Similar Song 2", "Similarity Score 2",
    "Similar Artist 3", "Similar Song 3", "Similarity Score 3"
)

similar_songs_df.show(5, truncate=20)

print("\nSchema DataFrame")
df.printSchema()

save_single_csv(songs_df, "data/processed/songs.csv")
save_single_csv(audio_features_df, "data/processed/audio_features.csv")
save_single_csv(activity_df, "data/processed/activity.csv")
save_single_csv(similar_songs_df, "data/processed/similar_tracks.csv")
save_single_csv(release_info_df, "data/processed/release_info.csv")