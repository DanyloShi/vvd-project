import os
import shutil
from glob import glob
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, rank, count

# 1. Ініціалізація Spark
spark = SparkSession.builder.appName("Roman_BusinessQuestions").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
RESULTS_DIR = PROJECT_ROOT / "results" / "kormyliuk"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# Зручна функція для збереження результату в один .csv файл
def save_result_csv(df, output_file):
    temp_dir = output_file + "_tmp"
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    if os.path.exists(output_file): os.remove(output_file)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_dir)
    part_file = glob(os.path.join(temp_dir, "part-*.csv"))[0]
    shutil.move(part_file, output_file)
    shutil.rmtree(temp_dir)


print("--- Завантаження підготовлених даних ---")
# Зчитуємо дані з папки data/processed/ (ці файли генерує data_preprocessing.py)
songs_df = spark.read.csv("data/processed/songs.csv", header=True, inferSchema=True)
release_info_df = spark.read.csv("data/processed/release_info.csv", header=True, inferSchema=True)
audio_features_df = spark.read.csv("data/processed/audio_features.csv", header=True, inferSchema=True)
activity_df = spark.read.csv("data/processed/activity.csv", header=True, inferSchema=True)

print("\n" + "=" * 50)
print("РОЗРАХУНОК БІЗНЕС-ПИТАНЬ")
print("=" * 50)

# 1. Які пісні мають популярність (Popularity) вище 80? (Вимога: filter)
q1 = release_info_df.filter(col("Popularity") > 80)
print("\n--- План виконання Q1 ---")
q1.explain()
save_result_csv(q1, str(RESULTS_DIR / "q1_high_popularity.csv"))

# 2. Високоенергійні пісні (Energy > 0.8), які підходять для вечірки (Good for Party == 1). (Вимоги: join, filter)
q2 = audio_features_df.join(activity_df, "song_id", "inner") \
    .filter((col("Energy") > 0.8) & (col("Good for Party") == 1))
print("\n--- План виконання Q2 ---")
q2.explain()
save_result_csv(q2, str(RESULTS_DIR / "q2_party_songs.csv"))

# 3. Скільки пісень випустив кожен артист (вивести тих, у кого > 3 пісень). (Вимоги: group by, filter)
q3 = songs_df.groupBy("Artist(s)") \
    .agg(count("song_id").alias("total_songs")) \
    .filter(col("total_songs") > 3)
print("\n--- План виконання Q3 ---")
q3.explain()
save_result_csv(q3, str(RESULTS_DIR / "q3_artist_song_count.csv"))

# 4. Яка середня танцювальність (Danceability) пісень для кожного альбому? (Вимоги: join, group by)
q4 = songs_df.join(audio_features_df, "song_id", "inner") \
    .groupBy("Album") \
    .agg(avg("Danceability").alias("avg_danceability"))
print("\n--- План виконання Q4 ---")
q4.explain()
save_result_csv(q4, str(RESULTS_DIR / "q4_album_danceability.csv"))

# 5. Яке місце займає пісня за популярністю серед усіх пісень свого артиста? (Вимоги: window function)
window_artist = Window.partitionBy("Artist(s)").orderBy(col("Popularity").desc())
q5 = songs_df.join(release_info_df, "song_id", "inner") \
    .withColumn("popularity_rank", rank().over(window_artist)) \
    .select("Artist(s)", "song", "Popularity", "popularity_rank")
print("\n--- План виконання Q5 ---")
q5.explain()
save_result_csv(q5, str(RESULTS_DIR / "q5_artist_popularity_rank.csv"))

# 6. Знайти пісні, чия популярність вища за середню глобальну популярність бази. (Вимоги: window function)
window_global = Window.partitionBy()
q6 = release_info_df.withColumn("avg_global_popularity", avg("Popularity").over(window_global)) \
    .filter(col("Popularity") > col("avg_global_popularity")) \
    .select("song_id", "Popularity", "avg_global_popularity")
print("\n--- План виконання Q6 ---")
q6.explain()
save_result_csv(q6, str(RESULTS_DIR / "q6_above_avg_popularity.csv"))

print(f"\nУСПІХ! Всі результати збережено у папку {RESULTS_DIR}/")
