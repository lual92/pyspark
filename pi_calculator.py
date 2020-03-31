def pi(N):
    import random
    result = 0
    for i in range(N):
        x = random.random()
        y = random.random()
        r = (x ** 2 + y ** 2)
        if r <= 1:
            result += 1
    return (result / N) * 4


print(pi(10000))