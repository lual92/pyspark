from operator import add

import pandas as pd
import unidecode
from functions import create_session
import re

def lector(archivo):
    with open(archivo, 'r') as libro:
        return libro.read().lower()


def acentos(palabras_relato):
    palabras_corregidas = []
    for i in palabras_relato:
        i = unidecode.unidecode(i)
        palabras_corregidas.append(i)
    return palabras_corregidas

def acento(palabra):
    palabra = unidecode.unidecode(palabra)
    return palabra


def contador_palabras(lista_contenido):
    tokens = '[\W0-9]+'
    lista_contenido = re.split(tokens, lista_contenido)
    return acentos(lista_contenido)


def useless_words(lista_palabras):
    stpwrds = lector('stopwords.txt')
    stpwrds = contador_palabras(stpwrds)
    return list(filter((lambda x: x not in stpwrds), lista_palabras))


def useless_word(palabra):
    stpwrds = lector('stopwords.txt')
    stpwrds = contador_palabras(stpwrds)
    return palabra not in stpwrds


def frecuencias(lista_contenido):
    relato = pd.DataFrame(lista_contenido, columns=['palabras'])
    return relato['palabras'].value_counts().reset_index()


def contar_palabras_spark(nombre_archivo):

    txt = lector(nombre_archivo)
    rdd = create_session()

    txt_tratado = rdd.sparkContext.parallelize([txt]) \
        .flatMap(contador_palabras) \
        .map(acento) \
        .filter(lambda palabra: useless_word(palabra)) \
        .map(lambda palabra: (palabra, 1)) \
        .reduceByKey(add) \
        .map(lambda tupla: (tupla[1], tupla[0])) \
        .sortByKey(False) \
        .map(lambda tupla: (tupla[1], tupla[0])) \
        .collect()

    rdd.stop()
    return txt_tratado


# frecs = contar_palabras_spark('platano.txt')