import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

INPUT_PATH = "/app/AllCombined.txt"
OUTPUT_DIR = "/tmp/out_top_tokens"
TOP_N = 10
MIN_LEN = 4

TOKEN_RE = re.compile(r"[A-Za-z0-9]+", re.UNICODE)

def simple_tokenize(line):
    tokens = (t.lower() for t in TOKEN_RE.findall(line))
    return [t for t in tokens if len(t) >= MIN_LEN]

def main():
    spark = (
        SparkSession.builder
        .appName("LR3-TF")
        .getOrCreate()
    )
    sc = spark.sparkContext

    lines = sc.textFile(INPUT_PATH)

    pairs = lines.flatMap(simple_tokenize).map(lambda token: (token, 1))
    counts = pairs.reduceByKey(lambda a, b: a + b)

    df = counts.toDF(["token", "tf"]).orderBy(desc("tf"), col("token"))
    top_df = df.limit(TOP_N)

    top_df.show()

    (
        top_df.coalesce(1)
              .write.mode("overwrite")
              .option("header", True)
              .csv(OUTPUT_DIR)
    )

    spark.stop()

if __name__ == "__main__":
    main()
