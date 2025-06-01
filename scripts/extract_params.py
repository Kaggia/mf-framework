import re

# Testo di input
text = """
"""

# Estrazione di tutte le stringhe comprese tra < e >
matches = re.findall(r'<(.*?)>', text)

# Unione delle stringhe in un'unica stringa separata da ;
result = ';'.join(matches)

# Visualizzazione del risultato
print(result)
