def lector(archivo):
    with open(archivo, 'r')as libro:
        return libro.read()


def contador_palabras(lista_contenido):
    import re
    tokens = '[\W0-9]+'
    lista_contenido = re.split(tokens, lista_contenido)
    return len(lista_contenido), lista_contenido


print(contador_palabras(lector('platano.txt')))