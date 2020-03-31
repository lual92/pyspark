import pandas as pd


def lector(archivo):
    with open(archivo, 'r') as libro:
        return libro.read().lower()


def acentos(palabras_relato):
    import unidecode
    palabras_corregidas = []
    for i in palabras_relato:
        i = unidecode.unidecode(i)
        palabras_corregidas.append(i)
    return palabras_corregidas


def contador_palabras(lista_contenido):
    import re
    tokens = '[\W0-9]+'
    lista_contenido = re.split(tokens, lista_contenido)
    return len(lista_contenido), acentos(lista_contenido)


def useless_words(lista_palabras):
    stpwrds = lector('stopwords.txt')
    stpwrds = contador_palabras(stpwrds)[1]
    return list(filter((lambda x: True if x not in stpwrds else False), lista_palabras))


def frecuencias(lista_contenido):
    relato = pd.DataFrame(lista_contenido, columns=['palabras'])
    return relato['palabras'].value_counts()