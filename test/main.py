import pandas as pd
import antrapi
import os
import sys
import json

def clean_results_folder():
    # Elimina tutti i file CSV nella cartella
    print(os.getcwd())
    for filename in os.listdir("../test/results/"):
        if filename.endswith('.csv'):
            file_path = os.path.join("../test/results/", filename)
            os.remove(file_path)
            print(f'Eliminato: {file_path}')

    print('Tutti i file CSV sono stati eliminati.')

if __name__ == "__main__":
    #Clean results folder
    clean_results_folder()

    print("Test Started.")
    os.chdir("../src")
    sys.path.append(os.getcwd())

    test_result = antrapi.model(
        event=json.load(open("../test/data/antrapi_test.json", "r")))
    
    output_result = test_result['outputs']
    rules_result     = test_result['rules']

    os.chdir("../test/results/")

    for inf_result in output_result:
        inf_result[list(inf_result.keys())[0]].to_csv(f"{list(inf_result.keys())[0]}_result.csv")

    for rule_result in rules_result:
        rule_result[list(rule_result.keys())[0]].to_csv(f"{list(rule_result.keys())[0]}_result.csv")

    print("Test Done.")
