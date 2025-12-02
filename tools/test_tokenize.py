from indexes.inverted_index import tokenize

q = 'Restaurante tradicional de la casa: platos caseros, pollo a la brasa y guisos al estilo familiar. Ambiente acogedor.'
print("Query:")
print(q)
print("Tokens (do_stem=True):")
print(tokenize(q, do_stem=True))
print("Tokens (do_stem=False):")
print(tokenize(q, do_stem=False))
