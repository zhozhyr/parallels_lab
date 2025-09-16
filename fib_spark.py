import sys
from pyspark.sql import SparkSession

"""
PySpark приложение, которое в client-режиме считает N-е число Фибоначчи и печатает результат.
Используется быстрый "fast doubling" алгоритм — O(log N).

Пример:
  spark-submit --master local[*] --deploy-mode client fib_spark.py 10
"""


def fast_doubling_fib(n: int) -> int:
    # Возвращает (F(n), F(n+1))
    if n == 0:
        return (0, 1)
    a, b = fast_doubling_fib(n >> 1)
    c = a * ((b << 1) - a)  # F(2k)   = F(k) * (2*F(k+1) - F(k))
    d = a * a + b * b  # F(2k+1) = F(k)^2 + F(k+1)^2
    if n & 1:
        return (d, c + d)
    else:
        return (c, d)


def main():
    if len(sys.argv) != 2:
        print("Usage: spark-submit --master local[*] --deploy-mode client fib_spark.py <N>")
        sys.exit(1)
    try:
        n = int(sys.argv[1])
        if n < 0:
            raise ValueError
    except ValueError:
        print("N must be a non-negative integer")
        sys.exit(2)

    spark = SparkSession.builder.appName("FibonacciApp").getOrCreate()

    # Само вычисление на драйвере.
    fib_n, _ = fast_doubling_fib(n)

    print(f"F({n}) = {fib_n}")

    spark.stop()


if __name__ == "__main__":
    main()
