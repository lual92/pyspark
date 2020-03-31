def lector(archivo):
    with open(archivo, 'r')as libro:
        return libro.read()


def acentos(palabras_relato):
    import unidecode
    return unidecode.unidecode(palabras_relato)


def contador_palabras(lista_contenido):
    import re
    tokens = '[\W0-9]+'
    lista_contenido = re.split(tokens, lista_contenido)
    return len(lista_contenido), acentos(str(lista_contenido))