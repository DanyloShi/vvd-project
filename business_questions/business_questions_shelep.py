from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, year, desc, round as spark_round, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
import os
import shutil
from glob import glob


spark = SparkSession.builder \
    .appName("SpotifyBusinessQuestions") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")

os.makedirs(RESULTS_DIR, exist_ok=True)


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


def explain_and_save(df, question_name, file_name):
    print("\n" + "=" * 80)
    print(question_name)
    print("=" * 80)

    df.explain(mode="formatted")
    df.show(20, truncate=False)

    save_single_csv(df, os.path.join(RESULTS_DIR, file_name))


songs_df = spark.read.option("header", True).option("inferSchema", True).csv(
    os.path.join(PROCESSED_DIR, "songs.csv")
)

genres_df = spark.read.option("header", True).option("inferSchema", True).csv(
    os.path.join(PROCESSED_DIR, "genres.csv")
)

song_genres_df = spark.read.option("header", True).option("inferSchema", True).csv(
    os.path.join(PROCESSED_DIR, "song_genres.csv")
)

release_info_df = spark.read.option("header", True).option("inferSchema", True).csv(
    os.path.join(PROCESSED_DIR, "release_info.csv")
)

audio_features_df = spark.read.option("header", True).option("inferSchema", True).csv(
    os.path.join(PROCESSED_DIR, "audio_features.csv")
)

activity_df = spark.read.option("header", True).option("inferSchema", True).csv(
    os.path.join(PROCESSED_DIR, "activity.csv")
)


# ----------------------------------------------------------------------
# 1. Середня популярність кожного альбому певного артиста
# ----------------------------------------------------------------------

ARTIST_NAME = "Taylor Swift"

q1 = (
    songs_df
    .join(release_info_df, "song_id", "inner")
    .filter(col("Artist(s)") == ARTIST_NAME)
    .groupBy("Artist(s)", "Album")
    .agg(
        spark_round(avg("Popularity"), 2).alias("avg_popularity"),
        count("song_id").alias("songs_count")
    )
    .orderBy(desc("avg_popularity"))
)

explain_and_save(
    q1,
    "Q1. Середня популярність кожного альбому певного артиста",
    "q1_avg_album_popularity_by_artist.csv"
)


# ----------------------------------------------------------------------
# 2. Топ-5 жанрів серед пісень для тренувань
# ----------------------------------------------------------------------

q2 = (
    activity_df
    .join(song_genres_df, "song_id", "inner")
    .join(genres_df, "genre_id", "inner")
    .filter(col("Good for Exercise") == 1)
    .groupBy("genre_name")
    .agg(count("song_id").alias("songs_count"))
    .orderBy(desc("songs_count"))
    .limit(5)
)

explain_and_save(
    q2,
    "Q2. Топ-5 жанрів серед пісень для тренувань",
    "q2_top5_workout_genres.csv"
)


# ----------------------------------------------------------------------
# 3. Топ-5 найпопулярніших пісень у кожному жанрі
# ----------------------------------------------------------------------

genre_popularity_df = (
    songs_df
    .join(release_info_df, "song_id", "inner")
    .join(song_genres_df, "song_id", "inner")
    .join(genres_df, "genre_id", "inner")
    .select(
        "genre_name",
        "Artist(s)",
        "song",
        "Album",
        "Popularity"
    )
)

genre_window = Window.partitionBy("genre_name").orderBy(desc("Popularity"))

q3 = (
    genre_popularity_df
    .withColumn("rank_in_genre", row_number().over(genre_window))
    .filter(col("rank_in_genre") == 1)
    .orderBy("genre_name")
)

explain_and_save(
    q3,
    "Q3. Топ-1 пісня у кожному жанрі",
    "q3_top1_songs_by_genre.csv"
)


# ----------------------------------------------------------------------
# 4. Середні аудіо-характеристики Energy та Danceability по жанрах
# ----------------------------------------------------------------------

q4 = (
    audio_features_df
    .join(song_genres_df, "song_id", "inner")
    .join(genres_df, "genre_id", "inner")
    .groupBy("genre_name")
    .agg(
        spark_round(avg("Energy"), 3).alias("avg_energy"),
        spark_round(avg("Danceability"), 3).alias("avg_danceability"),
        count("song_id").alias("songs_count")
    )
    .orderBy(desc("avg_energy"))
)

explain_and_save(
    q4,
    "Q4. Середні аудіо-характеристики Energy та Danceability по жанрах",
    "q4_avg_audio_features_by_genre.csv"
)


# ----------------------------------------------------------------------
# 5. Найбільш поширена емоція та середній Positiveness для кожного року
# ----------------------------------------------------------------------

year_emotion_df = (
    release_info_df
    .join(audio_features_df, "song_id", "inner")
    .withColumn("year", year(col("Release Date")))
    .filter(col("year").isNotNull())
)

emotion_counts_df = (
    year_emotion_df
    .groupBy("year", "emotion")
    .agg(count("song_id").alias("emotion_count"))
)

emotion_window = Window.partitionBy("year").orderBy(desc("emotion_count"))

most_common_emotion_df = (
    emotion_counts_df
    .withColumn("emotion_rank", dense_rank().over(emotion_window))
    .filter(col("emotion_rank") == 1)
    .select(
        "year",
        col("emotion").alias("most_common_emotion"),
        "emotion_count"
    )
)

avg_positiveness_df = (
    year_emotion_df
    .groupBy("year")
    .agg(
        spark_round(avg("Positiveness"), 3).alias("avg_positiveness")
    )
)

q5 = (
    most_common_emotion_df
    .join(avg_positiveness_df, "year", "inner")
    .orderBy("year")
)

explain_and_save(
    q5,
    "Q5. Найпоширеніша емоція та середній Positiveness для кожного року",
    "q5_emotion_and_positiveness_by_year.csv"
)


# ----------------------------------------------------------------------
# 6. Топ-15 пісень для роботи/навчання та релаксації
# ----------------------------------------------------------------------

q6 = (
    activity_df
    .join(songs_df, "song_id", "inner")
    .join(release_info_df, "song_id", "inner")
    .filter(
        (col("Good for Work/Study") == 1) &
        (col("Good for Relaxation/Meditation") == 1)
    )
    .select(
        "Artist(s)",
        "song",
        "Album",
        "Popularity",
        "emotion"
    )
    .orderBy(desc("Popularity"))
    .limit(15)
)

explain_and_save(
    q6,
    "Q6. Топ-15 пісень для роботи/навчання та релаксації",
    "q6_top15_work_and_relaxation_songs.csv"
)


spark.stop()
