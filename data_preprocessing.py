from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, to_date, regexp_replace, trim, \
    concat_ws, sha2, explode, split, lower, array, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import os
import shutil
from glob import glob

spark = SparkSession.builder \
    .appName("SpotifyDataAnalysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def save_single_csv(df, output_file):
    temp_dir = output_file + "_tmp"
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    if os.path.exists(output_file): os.remove(output_file)
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

df = spark.read.csv(
    "data/raw/spotify_dataset.csv", 
    header=True, 
    schema=schema, 
    multiLine=True,    
    quote='"',       
    escape='"',       
    charToEscapeQuoteEscaping='\\'
)
df = df.cache() 

print("\nStatistical analysis")
print(f"Total number of records: {df.count()}")
print(f"Number of signs: {len(df.columns)}")

numeric_cols = [c for c, t in df.dtypes if t in ['int', 'float', 'integer']]
print("\nStatistics of numerical features:")
df.select(numeric_cols).summary().show(vertical=True)

df = df.withColumn("Release Date", regexp_replace(col("Release Date"), r"(\d+)(st|nd|rd|th)", "$1"))

df = df.withColumn("Release Date", to_date(col("Release Date"), "d MMMM yyyy"))
df = df.withColumn("Loudness (db)", regexp_replace(col("Loudness (db)"), "[^0-9.-]", "").cast(FloatType()))
df = df.withColumn("Explicit", when(col("Explicit") == "True", 1).otherwise(0))

initial_count = df.count()
df = df.dropDuplicates(["Artist(s)", "song", "Album"])
print(f"Duplicates removed: {initial_count - df.count()}")

print("\nMissing value analysis:")
all_columns = df.dtypes
batch_size = 10
for i in range(0, len(all_columns), batch_size):
    batch = all_columns[i : i + batch_size]
    null_checks = [count(when((isnan(col(c)) if t in ["float", "double"] else col(c).isNull()) | col(c).isNull(), c)).alias(c) 
                   for c, t in batch]
    df.select(null_checks).show(vertical=True)

before = df.count()

df = df.dropna(subset=["Artist(s)", "song", "Genre", "Album"])
df = df.dropna(subset=numeric_cols)
print("Missing value filled with baseline values")

after = df.count()

print(f"Dropped {before - after} rows")

df = df.drop("text")
print("'text' tag removed")

df = df.withColumn(
    "song_id",
    sha2(concat_ws("||", col("Artist(s)"), col("song"), col("Album")), 256)
)

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

songs_df = df.select("song_id", "Artist(s)", "song", "Album")
release_info_df = df.select("song_id", "Release Date", "Length", "emotion", "Popularity")
audio_features_df = df.select("song_id", "Key", "Tempo", "Loudness (db)", "Energy", "Danceability", "Positiveness", "Speechiness", "Liveness", "Acousticness", "Instrumentalness")
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

save_single_csv(songs_df, "data/processed/songs.csv")
save_single_csv(genres_df, "data/processed/genres.csv")
save_single_csv(song_genres_df, "data/processed/song_genres.csv")
save_single_csv(release_info_df, "data/processed/release_info.csv")
save_single_csv(audio_features_df, "data/processed/audio_features.csv")
save_single_csv(similar_songs_df, "data/processed/similar_songs.csv")
save_single_csv(activity_df, "data/processed/activity.csv")