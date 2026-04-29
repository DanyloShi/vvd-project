import os
import shutil
from glob import glob
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

OUTPUT_DIR = PROJECT_ROOT / "results" / "kysil"
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
    .appName("SpotifyMyBusinessQuestions")
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
    .filter(F.col("Release Date").isNotNull())
    .withColumn("release_year", F.year(F.col("Release Date")))
    .groupBy("release_year")
    .agg(
        F.count("*").alias("tracks_count"),
        F.round(F.avg("Loudness (db)"), 4).alias("avg_loudness_db"),
        F.round(F.max("Loudness (db)"), 4).alias("max_loudness_db")
    )
    .filter(F.col("tracks_count") >= 50)
    .orderBy(F.desc("release_year"))
    .limit(1000)
)
save_question_result(
    "q1_loudness_by_year",
    "Як змінювалася середня гучність музики (Loudness db) по роках.",
    q1,
)

q2 = (
    curated_df
    .filter(F.col("Key").isNotNull())
    .filter(F.trim(F.col("Key")) != "")
    .groupBy("Key")
    .agg(
        F.count("*").alias("total_tracks"),
        F.round(F.avg("Popularity"), 2).alias("avg_popularity"),
        F.sum(F.when(F.col("Popularity") >= 80, 1).otherwise(0)).alias("hits_count")
    )
    .filter(F.col("total_tracks") >= 100)
    .orderBy(F.desc("avg_popularity"), F.desc("hits_count"))
    .limit(100)
)
save_question_result(
    "q2_most_successful_key",
    "Яка музична тональність (Key) має найвищу середню популярність та найбільше хітів.",
    q2,
)

q3 = (
    curated_df.select("song_id", "Popularity", "Artist(s)", "song")
    .join(
        release_df.select("song_id", "Length_seconds", "Release Date"),
        on="song_id",
        how="inner"
    )
    .filter(F.col("Release Date").isNotNull())
    .withColumn("release_year", F.year(F.col("Release Date")))
    .filter(F.col("release_year") >= 2015)
    .withColumn(
        "popularity_tier",
        F.when(F.col("Popularity") >= 80, "1. Hit (>=80)")
        .when(F.col("Popularity") >= 50, "2. Average (50-79)")
        .otherwise("3. Low (<50)")
    )
    .groupBy("popularity_tier")
    .agg(
        F.count("*").alias("tracks_count"),
        F.round(F.avg("length_seconds"), 2).alias("avg_length_sec"),
        F.round(F.avg("length_seconds") / 60, 2).alias("avg_length_min")
    )
    .orderBy("popularity_tier")
    .limit(10)
)
save_question_result(
    "q3_long_songs_popularity_nowadays",
    "Середня тривалість пісень (з 2015 року) в залежності від рівня їхньої популярності.",
    q3,
)

q4 = (
    song_genres_df
    .join(
        curated_df.select("song_id", "Instrumentalness"),
        on="song_id",
        how="inner"
    )
    .filter(F.col("Instrumentalness").isNotNull())
    .groupBy("genre_name")
    .agg(
        F.countDistinct("song_id").alias("tracks_count"),
        F.round(F.avg("Instrumentalness"), 4).alias("avg_instrumentalness")
    )
    .filter(F.col("tracks_count") >= 100)
    .orderBy(F.desc("avg_instrumentalness"))
    .limit(5)
)
save_question_result(
    "q4_top5_instrumental_genres",
    "Топ-5 жанрів за найвищим середнім показником Instrumentalness.",
    q4,
)

q5 = (
    curated_df
    .filter((F.col("Danceability") > 90) & (F.col("Popularity") < 30))
    .select(
        "Artist(s)",
        "song",
        "Danceability",
        "Popularity",
        "Genre",
        "Release Date"
    )
    .orderBy(F.desc("Danceability"), F.asc("Popularity"))
    .limit(10)
)
save_question_result(
    "q5_hidden_dance_gems",
    "10 треків з Danceability > 90, але Popularity < 30 (приховані перлини).",
    q5,
)

q6 = (
    similar_df
    .filter(F.col("similar_rank") == 1)
    .filter(F.col("similar_artist").isNotNull())
    .filter(F.trim(F.col("similar_artist")) != "")
    .groupBy("similar_artist")
    .agg(
        F.count("*").alias("recommended_count")
    )
    .orderBy(F.desc("recommended_count"))
    .limit(2000)
)
save_question_result(
    "q6_most_frequent_similar_artist_1",
    "Топ виконавців, які найчастіше зустрічаються в колонці Similar Artist 1.",
    q6,
)

print("\nTransformation stage completed.")
print(f"CSV results saved to: {OUTPUT_DIR}")
print(f"Execution plans saved to: {PLANS_DIR}")

spark.stop()
