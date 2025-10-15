def fib(n: int) -> int:
    """
    Вычисляет n-е число Фибоначчи.
    F(0) = 0, F(1) = 1
    """
    if n < 0:
        raise ValueError("N должно быть неотрицательным")
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a
