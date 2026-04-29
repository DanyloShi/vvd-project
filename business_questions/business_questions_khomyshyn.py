from glob import glob
import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

SONGS_FILE = str(PROCESSED_DIR / "songs.csv")
GENRES_FILE = str(PROCESSED_DIR / "genres.csv")
SONG_GENRES_FILE = str(PROCESSED_DIR / "song_genres.csv")
ACTIVITY_FILE = str(PROCESSED_DIR / "activity.csv")
AUDIO_FILE = str(PROCESSED_DIR / "audio_features.csv")
RELEASE_FILE = str(PROCESSED_DIR / "release_info.csv")
SIMILAR_FILE = str(PROCESSED_DIR / "similar_songs.csv")

OUTPUT_DIR = PROJECT_ROOT / "results" / "khomyshyn"
PLANS_DIR = OUTPUT_DIR / "plans"


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


def normalize_bom_column(df):
    for column_name in df.columns:
        if column_name.startswith("\ufeff"):
            cleaned = column_name.lstrip("\ufeff")
            df = df.withColumnRenamed(column_name, cleaned)
    return df


def save_question_result(name, description, df):
    print(f"\n{name}: {description}")
    print(f"{name} execution plan (explain):")
    df.explain(mode="formatted")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PLANS_DIR.mkdir(parents=True, exist_ok=True)
    plan_text = df._jdf.queryExecution().toString()
    (PLANS_DIR / f"{name}_plan.txt").write_text(plan_text, encoding="utf-8")
    save_single_csv(df, str(OUTPUT_DIR / f"{name}.csv"))


spark = (
    SparkSession.builder
    .appName("SpotifyTransformationStage")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PLANS_DIR.mkdir(parents=True, exist_ok=True)


for old_csv in OUTPUT_DIR.glob("q*.csv"):
    old_csv.unlink(missing_ok=True)
for old_plan in PLANS_DIR.glob("q*_plan.txt"):
    old_plan.unlink(missing_ok=True)

songs_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(SONGS_FILE)
)
songs_df = normalize_bom_column(songs_df)

genres_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(GENRES_FILE)
)
song_genres_map_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(SONG_GENRES_FILE)
)
activity_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ACTIVITY_FILE)
)
audio_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(AUDIO_FILE)
)
release_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RELEASE_FILE)
    .withColumn("Release Date", F.to_date(F.col("Release Date"), "yyyy-MM-dd"))
)

release_df = release_df.withColumn(
    "length_seconds",
    F.when(
        F.col("Length").rlike(r"^\d{1,2}:\d{2}$"),
        F.split(F.col("Length"), ":").getItem(0).cast("int") * 60
        + F.split(F.col("Length"), ":").getItem(1).cast("int")
    ).otherwise(F.lit(None).cast("int"))
)

similar_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(SIMILAR_FILE)
)

song_genres_df = (
    song_genres_map_df
    .join(genres_df, on="genre_id", how="inner")
    .select("song_id", F.trim(F.lower(F.col("genre_name"))).alias("genre_name"))
    .filter(F.col("genre_name").isNotNull())
    .filter(F.col("genre_name") != "")
    .dropDuplicates()
)

song_genres_text_df = (
    song_genres_df
    .groupBy("song_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("genre_name"))).alias("Genre"))
)

similar_rank1_df = (
    similar_df
    .filter(F.col("similar_rank") == 1)
    .select("song_id", F.col("similar_artist").alias("Similar Artist 1"))
)

curated_df = (
    songs_df
    .join(release_df.select("song_id", "Release Date", "length_seconds", "Popularity"), on="song_id", how="left")
    .join(audio_df, on="song_id", how="left")
    .join(activity_df, on="song_id", how="left")
    .join(song_genres_text_df, on="song_id", how="left")
    .join(similar_rank1_df, on="song_id", how="left")
)

q1 = (
    curated_df
    .filter(F.col("Popularity") >= 70)
    .groupBy("Artist(s)")
    .agg(
        F.count("*").alias("hits_count"),
        F.round(F.avg("Popularity"), 2).alias("avg_popularity"),
        F.max("Popularity").alias("max_popularity"),
    )
    .orderBy(F.desc("hits_count"), F.desc("avg_popularity"))
    .limit(10)
)
save_question_result(
    "q1_top_artists_hits",
    "Які 10 виконавців мають найбільше треків з популярністю >= 70.",
    q1,
)

q2 = (
    song_genres_df
    .join(
        curated_df.select("song_id", "Tempo", "Energy", "Good for Running"),
        on="song_id",
        how="inner",
    )
    .filter(F.col("Good for Running") == 1)
    .groupBy("genre_name")
    .agg(
        F.countDistinct("song_id").alias("tracks_count"),
        F.round(F.avg("Tempo"), 2).alias("avg_tempo"),
        F.round(F.avg("Energy"), 2).alias("avg_energy"),
    )
    .filter(F.col("tracks_count") >= 50)
    .orderBy(F.desc("avg_tempo"), F.desc("avg_energy"))
    .limit(20)
)
save_question_result(
    "q2_running_genres_tempo_energy",
    "Які жанри для бігу мають найвищі середні Tempo та Energy.",
    q2,
)

genre_rank_window = Window.partitionBy("genre_name").orderBy(
    F.desc("Popularity"), F.desc("Danceability")
)
q3 = (
    song_genres_df
    .join(
        curated_df.select("song_id", "Artist(s)", "song", "Popularity", "Danceability"),
        on="song_id",
        how="inner",
    )
    .withColumn("genre_rank", F.row_number().over(genre_rank_window))
    .filter(F.col("genre_rank") <= 3)
    .select("genre_name", "genre_rank", "Artist(s)", "song", "Popularity", "Danceability")
    .orderBy("genre_name", "genre_rank")
)
save_question_result(
    "q3_top3_per_genre",
    "Топ-3 треки в кожному жанрі за популярністю (window row_number).",
    q3,
)

q4 = (
    curated_df
    .filter(F.col("Album").isNotNull())
    .filter(F.trim(F.col("Album")) != "")
    .groupBy("Album")
    .agg(
        F.count("*").alias("tracks_count"),
        F.round(F.avg("Popularity"), 2).alias("avg_popularity"),
        F.max("Popularity").alias("max_popularity"),
    )
    .filter(F.col("tracks_count") >= 5)
    .orderBy(F.desc("avg_popularity"), F.desc("tracks_count"))
    .limit(20)
)
save_question_result(
    "q4_top_albums_avg_popularity",
    "Які альбоми мають найвищу середню популярність (мінімум 5 треків).",
    q4,
)

year_rank_window = Window.partitionBy("release_year").orderBy(
    F.desc("Popularity"), F.asc("song")
)
q5 = (
    release_df
    .join(curated_df.select("song_id", "Artist(s)", "song"), on="song_id", how="inner")
    .filter(F.col("Release Date").isNotNull())
    .withColumn("release_year", F.year(F.col("Release Date")))
    .withColumn("year_rank", F.row_number().over(year_rank_window))
    .filter(F.col("year_rank") <= 1)
    .select("release_year", "year_rank", "Artist(s)", "song", "Popularity", "length_seconds")
    .orderBy("release_year", "year_rank")
)
save_question_result(
    "q5_top1_by_year",
    "Топ-1 трек за популярністю в кожному році (window row_number).",
    q5,
)

similar_long_df = (
    similar_df
    .select("song_id", "similarity_score")
    .filter(F.col("similarity_score").isNotNull())
)
q6 = (
    similar_long_df
    .join(song_genres_df, on="song_id", how="inner")
    .groupBy("genre_name")
    .agg(
        F.countDistinct("song_id").alias("songs_with_similars"),
        F.round(F.avg("similarity_score"), 4).alias("avg_similarity_score"),
    )
    .filter(F.col("songs_with_similars") >= 300)
    .orderBy(F.desc("avg_similarity_score"), F.desc("songs_with_similars"))
)
save_question_result(
    "q6_genre_similarity_quality",
    "У яких жанрах рекомендації мають найвищий середній similarity score.",
    q6,
)

print("\nTransformation stage completed.")
print(f"CSV results saved to: {OUTPUT_DIR}")
print(f"Execution plans saved to: {PLANS_DIR}")

spark.stop()
