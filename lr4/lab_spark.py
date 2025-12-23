from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT_PLAYERS = "/app/players.csv"
INPUT_SALARIES = "/app/salaries_1985to2018.csv"
INPUT_SEASONS_STATS = "/app/Seasons_Stats.csv"

OUTPUT_PARQUET = "/tmp/out_parquet"
OUTPUT_TOP = "/tmp/out_top_tokens"

spark = SparkSession.builder \
    .appName("LR4_spark") \
    .getOrCreate()

players = (
    spark.read
    .option("header", "true")
    .csv(INPUT_PLAYERS)
)

salaries = (
    spark.read
    .option("header", "true")
    .csv(INPUT_SALARIES)
)

seasons_stats = (
    spark.read
    .option("header", "true")
    .csv(INPUT_SEASONS_STATS)
)

players_std = players.select(
    F.col("_id").alias("player_id"),
    F.col("name").alias("player_name")
)

salaries = (
    salaries
    .withColumn("salary", F.col("salary").cast("double"))
    .withColumn("season_year", F.col("season_end").cast("int"))
)

salaries_std = salaries.select(
    "player_id",
    "salary",
    "season_year",
    "season",
    "team",
    "league"
)


seasons_stats = seasons_stats.withColumn(
    "season_year", F.col("Year").cast("int")
)

seasons_stats = seasons_stats.withColumnRenamed("Player", "player_name")

for col_name in ["PTS", "TRB", "AST"]:
    if col_name in seasons_stats.columns:
        seasons_stats = seasons_stats.withColumn(col_name, F.col(col_name).cast("double"))


seasons_agg = seasons_stats.groupBy(
    "player_name", "season_year"
).agg(
    F.sum("PTS").alias("PTS"),
    F.sum("TRB").alias("TRB"),
    F.sum("AST").alias("AST")
)

seasons_agg = seasons_agg.withColumn(
    "efficiency",
    F.col("PTS") + F.col("TRB") + F.col("AST")
)

seasons_agg = seasons_agg.filter(F.col("efficiency") > 0)

stats_with_id = seasons_agg.join(
    players_std,
    on="player_name",
    how="left"
)

stats_salary = stats_with_id.join(
    salaries_std.select("player_id", "season_year", "salary"),
    on=["player_id", "season_year"],
    how="inner"
)

stats_salary = stats_salary.withColumn(
    "cost_per_eff",
    F.col("salary") / F.col("efficiency")
)

(
    stats_salary
    .select(
        "player_id",
        "player_name",
        "season_year",
        "PTS",
        "TRB",
        "AST",
        "efficiency",
        "salary",
        "cost_per_eff",
    )
    .write
    .mode("overwrite")
    .partitionBy("season_year")
    .parquet(OUTPUT_PARQUET)
)

eff_parquet = spark.read.parquet(OUTPUT_PARQUET)

w = Window.partitionBy("season_year").orderBy(F.col("cost_per_eff").asc())

ranked = eff_parquet.withColumn("rn", F.row_number().over(w))

top5_per_year = (
    ranked
    .filter(F.col("rn") <= 5)
    .select(
        "season_year",
        "rn",
        "player_id",
        "player_name",
        "salary",
        "efficiency",
        "cost_per_eff",
    )
    .orderBy("season_year", "rn")
)

top5_per_year.show(100, truncate=False)

(
    top5_per_year
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(OUTPUT_TOP)
)

spark.stop()
