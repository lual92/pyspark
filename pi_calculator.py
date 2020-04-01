import math
import numpy as np

def estimar_pi(N):
    import random
    result = 0
    for i in range(N):
        x = random.random()
        y = random.random()
        r = (x ** 2 + y ** 2)
        if r <= 1:
            result += 1
    return (result / N) * 4


from pyspark.sql import SparkSession


NUMERO_MAXIMO_CPUS = 3
SPARK_MASTER = "local["+str(NUMERO_MAXIMO_CPUS)+"]"


def estimar_pi_spark(calculos,numero_bloques):

    spark = SparkSession\
            .builder\
            .master(SPARK_MASTER)\
            .appName("EstimarPISpark")\
            .getOrCreate()

    calculos_por_bloque = int(calculos / numero_bloques)
    bloques = [calculos_por_bloque] * numero_bloques

    total = spark.sparkContext.parallelize(bloques, numero_bloques).map(estimar_pi).reduce(sumar)
    spark.stop()
    return total / numero_bloques


def sumar(a, b):
    return a + b

print(estimar_pi_spark(100*1000*1000, 3))