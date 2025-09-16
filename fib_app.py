import sys
from pyspark.sql import SparkSession
from fibonacci import fib

def main():
    if len(sys.argv) != 2:
        print("Usage: spark-submit --master local[*] --deploy-mode client fib_app.py <N>")
        sys.exit()
    try:
        n = int(sys.argv[1])
        if n < 0:
            raise ValueError
    except ValueError:
        print("N must be a non-negative integer")
        sys.exit()

    spark = SparkSession.builder.appName("FibonacciApp").getOrCreate()

    fib_n = fib(n)

    print(f"F({n}) = {fib_n}")

    spark.stop()


if __name__ == "__main__":
    main()
