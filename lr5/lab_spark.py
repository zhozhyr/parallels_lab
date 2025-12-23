from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, format_number

spark = (
    SparkSession.builder
    .appName("Lr5")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

csv_path = "Player - Salaries per Year (1990 - 2017).csv"

df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csv_path)
)

df = (
    df_raw
    .select(
        col("Season Start").alias("Season"),
        col("Player Name").alias("Player"),
        regexp_replace(
            trim(col(" Salary in $ ")),
            "[$,]",
            ""
        ).alias("salary_str")
    )
)

df = df.withColumn("salary", col("salary_str").cast("double")).drop("salary_str")

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.player_salaries (
        Season INT,
        Player STRING,
        salary DOUBLE
    )
    USING iceberg
    PARTITIONED BY (Season)
""")

start_year = 1990
end_year = 2017

for year in range(start_year, end_year + 1):
    df_year = df.filter(col("Season") == year)

    view_name = f"salaries_{year}"
    df_year.createOrReplaceTempView(view_name)

    merge_sql = f"""
        MERGE INTO local.db.player_salaries AS t
        USING {view_name} AS s
        ON t.Season = s.Season AND t.Player = s.Player
        WHEN MATCHED THEN
          UPDATE SET
            t.salary = s.salary
        WHEN NOT MATCHED THEN
          INSERT (Season, Player, salary)
          VALUES (s.Season, s.Player, s.salary)
    """

    spark.sql(merge_sql)

snapshots_df = spark.sql("""
    SELECT snapshot_id, committed_at, operation
    FROM local.db.player_salaries.snapshots
    ORDER BY committed_at DESC
""")
print("Список snapshot'ов Iceberg")
snapshots_df.show(truncate=False)

snapshot = snapshots_df.first()["snapshot_id"]

df_2016_snapshot = spark.sql(f"""
    SELECT *
    FROM local.db.player_salaries VERSION AS OF {snapshot}
    WHERE Season = 2016
""").withColumn("salary", format_number("salary", 0))

print("Пример данных за 2016 год")
df_2016_snapshot.show(20, truncate=False)

top10_2016_snapshot = spark.sql(f"""
    SELECT Player, salary
    FROM local.db.player_salaries VERSION AS OF {snapshot}
    WHERE Season = 2016
    ORDER BY salary DESC
    LIMIT 10
""").withColumn("salary", format_number("salary", 0))

print("Top 10 выскооплачиваемых игроков в 2016")
top10_2016_snapshot.show(truncate=False)

spark.stop()
