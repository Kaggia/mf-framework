import os

def conta_righe_di_codice(cartella):
    totali = 0
    for root, dirs, files in os.walk(cartella):
        for file in files:
            if file.endswith(".py"):  # Puoi modificare l'estensione per adattarti ai tuoi tipi di file
                filepath = os.path.join(root, file)
                with open(filepath, 'r', encoding='utf-8') as f:
                    totali += sum(1 for line in f if line.strip())
    return totali

os.system("clear")
cartella_da_contare = "../src/"
righe_totali_package = conta_righe_di_codice(cartella_da_contare)

cartella_da_contare = "../unittests/"
righe_totali_tests   = conta_righe_di_codice(cartella_da_contare)

print(
f"""
Counting lines in project
---------------------------------
Package lines :" {righe_totali_package}
Unittest lines:" {righe_totali_tests}
---------------------------------
Total lines   :" {righe_totali_package+righe_totali_tests}
---------------------------------
""")
