import os
import re
import shutil
from glob import glob
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, row_number, when, lower
from pyspark.sql.window import Window


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


def clean_column_name(name):
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    return name


def normalize_columns(df):
    for old_name in df.columns:
        new_name = clean_column_name(old_name)
        df = df.withColumnRenamed(old_name, new_name)

    if "artist_s" in df.columns:
        df = df.withColumnRenamed("artist_s", "artist")

    return df


def read_processed_csv(spark, path):
    return normalize_columns(
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


spark = (
    SparkSession.builder
    .appName("SpotifyBusinessQuestions")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
RESULTS_DIR = PROJECT_ROOT / "results" / "vulchyn"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# Зчитування вже підготовлених таблиць
songs_df = read_processed_csv(spark, "data/processed/songs.csv")
genres_df = read_processed_csv(spark, "data/processed/genres.csv")
song_genres_df = read_processed_csv(spark, "data/processed/song_genres.csv")
release_info_df = read_processed_csv(spark, "data/processed/release_info.csv")
audio_features_df = read_processed_csv(spark, "data/processed/audio_features.csv")
similar_songs_df = read_processed_csv(spark, "data/processed/similar_songs.csv")
activity_df = read_processed_csv(spark, "data/processed/activity.csv")


# Основна таблиця з інформацією про пісні
songs_main_df = (
    songs_df
    .join(release_info_df, "song_id", "inner")
)

# Якщо Explicit зчитався як True/False або рядок, приводимо до 0/1
if "explicit" in songs_main_df.columns:
    songs_main_df = songs_main_df.withColumn(
        "explicit_flag",
        when(lower(col("explicit").cast("string")).isin("true", "1", "yes"), 1).otherwise(0)
    )
else:
    songs_main_df = songs_main_df.withColumn("explicit_flag", when(col("song_id").isNotNull(), 0).otherwise(0))


# 1. Які 15 найпопулярніших пісень можна рекомендувати для навчання або роботи?
q1 = (
    songs_main_df
    .join(activity_df, "song_id", "inner")
    .filter(col("good_for_work_study") == 1)
    .filter(col("explicit_flag") == 0)
    .select(
        "artist",
        "song",
        "album",
        "popularity",
        "good_for_work_study",
        "explicit_flag"
    )
    .orderBy(desc("popularity"))
    .limit(15)
)

print("\nQ1: Top 15 songs for work or study")
q1.show(truncate=False)
q1.explain(True)
save_single_csv(q1, str(RESULTS_DIR / "q1_top_songs_for_work_study.csv"))


# 2. Які пісні з високою танцювальністю та позитивністю найкраще підходять для соціальних подій?
# У processed activity.csv немає поля Good for Social Gatherings, тому використано Good for Party
q2 = (
    songs_main_df
    .join(audio_features_df, "song_id", "inner")
    .join(activity_df, "song_id", "inner")
    .filter(col("danceability") >= 75)
    .filter(col("positiveness") >= 70)
    .filter(col("good_for_party") == 1)
    .select(
        "artist",
        "song",
        "popularity",
        "danceability",
        "positiveness",
        "good_for_party"
    )
    .orderBy(desc("popularity"))
    .limit(15)
)

print("\nQ2: Danceable and positive songs for social events")
q2.show(truncate=False)
q2.explain(True)
save_single_csv(q2, str(RESULTS_DIR / "q2_dance_positive_social_songs.csv"))


# 3. Які жанри мають найбільшу частку explicit-контенту?
q3 = (
    song_genres_df
    .join(genres_df, "genre_id", "inner")
    .join(songs_main_df, "song_id", "inner")
    .groupBy("genre_name")
    .agg(
        count("*").alias("songs_count"),
        avg("explicit_flag").alias("explicit_share")
    )
    .filter(col("songs_count") >= 5)
    .orderBy(desc("explicit_share"), desc("songs_count"))
    .limit(10)
)

print("\nQ3: Genres with the highest explicit content share")
q3.show(truncate=False)
q3.explain(True)
save_single_csv(q3, str(RESULTS_DIR / "q3_genres_by_explicit_share.csv"))


# 4. Які жанри мають найвищу середню енергійність та темп?
q4 = (
    song_genres_df
    .join(genres_df, "genre_id", "inner")
    .join(audio_features_df, "song_id", "inner")
    .groupBy("genre_name")
    .agg(
        count("*").alias("songs_count"),
        avg("energy").alias("avg_energy"),
        avg("tempo").alias("avg_tempo")
    )
    .filter(col("songs_count") >= 5)
    .orderBy(desc("avg_energy"), desc("avg_tempo"))
    .limit(10)
)

print("\nQ4: Genres with the highest average energy and tempo")
q4.show(truncate=False)
q4.explain(True)
save_single_csv(q4, str(RESULTS_DIR / "q4_high_energy_tempo_genres.csv"))


# 5. Які топ-5 найпопулярніші пісні кожного настрою/emotion?
emotion_window = Window.partitionBy("emotion").orderBy(desc("popularity"))

q5 = (
    songs_main_df
    .filter(col("emotion").isNotNull())
    .filter(col("emotion") != "")
    .withColumn("rank_in_emotion", row_number().over(emotion_window))
    .filter(col("rank_in_emotion") <= 5)
    .select(
        "emotion",
        "rank_in_emotion",
        "artist",
        "song",
        "album",
        "popularity"
    )
    .orderBy("emotion", "rank_in_emotion")
)

print("\nQ5: Top 5 songs by emotion")
q5.show(50, truncate=False)
q5.explain(True)
save_single_csv(q5, str(RESULTS_DIR / "q5_top5_songs_by_emotion.csv"))


# 6. Які виконавці мають найкращий середній рейтинг популярності, якщо мають мінімум 3 пісні в датасеті?
artist_popularity_df = (
    songs_main_df
    .groupBy("artist")
    .agg(
        count("*").alias("songs_count"),
        avg("popularity").alias("avg_popularity")
    )
    .filter(col("songs_count") >= 3)
)

artist_rating_window = Window.orderBy(desc("avg_popularity"))

q6 = (
    artist_popularity_df
    .withColumn("artist_rank", row_number().over(artist_rating_window))
    .filter(col("artist_rank") <= 20)
    .select(
        "artist_rank",
        "artist",
        "songs_count",
        "avg_popularity"
    )
    .orderBy("artist_rank")
)

print("\nQ6: Top artists by average popularity")
q6.show(50, truncate=False)
q6.explain(True)
save_single_csv(q6, str(RESULTS_DIR / "q6_top_artists_by_avg_popularity.csv"))


spark.stop()
