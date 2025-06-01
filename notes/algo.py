import re

def clean_string(s):
    # Funzione per normalizzare i nomi rimuovendo underscore e convertendo in minuscolo
    return re.sub(r'[_]', '', s).lower()

def levenshtein_distance(str1, str2):
    # Inizializza la matrice di distanza
    len_str1 = len(str1) + 1
    len_str2 = len(str2) + 1

    # Crea una matrice vuota
    dp = [[0 for _ in range(len_str2)] for _ in range(len_str1)]

    # Inizializza i valori della prima riga e della prima colonna
    for i in range(len_str1):
        dp[i][0] = i
    for j in range(len_str2):
        dp[0][j] = j

    # Calcola la distanza di Levenshtein
    for i in range(1, len_str1):
        for j in range(1, len_str2):
            cost = 0 if str1[i-1] == str2[j-1] else 1
            dp[i][j] = min(dp[i-1][j] + 1,        # Cancellazione
                           dp[i][j-1] + 1,        # Inserzione
                           dp[i-1][j-1] + cost)   # Sostituzione

    # La distanza finale si trova in dp[len_str1-1][len_str2-1]
    return dp[-1][-1]

def find_best_match(column_name, class_names):
    # Normalizza il nome della colonna
    normalized_col = clean_string(column_name)
    
    # Calcola la distanza di Levenshtein tra il nome della colonna e ogni nome di classe
    best_match = None
    lowest_distance = float('inf')
    
    for class_name in class_names:
        normalized_class = clean_string(class_name)
        distance = levenshtein_distance(normalized_col, normalized_class)
        
        if distance < lowest_distance:
            lowest_distance = distance
            best_match = class_name
            
    return best_match, lowest_distance

# Lista delle colonne
columns = ['sampledate', 'avg_temp', 'm4g3in_tr43emp', 'max_temp', 'leaf_wetness']

# Lista dei nomi delle classi
class_names = ['SampleDate', 'MinimumTemperature', 'MaximumTemperature', 'AverageTemperature']

# Mappatura delle colonne alle classi
column_class_mapping = {}

for column in columns:
    best_match_class, distance = find_best_match(column, class_names)
    column_class_mapping[column] = (best_match_class, distance)

# Risultati
for column, (cls, distance) in column_class_mapping.items():
    print(f"Column '{column}' is best matched with class '{cls}' with a Levenshtein distance of {distance}")
