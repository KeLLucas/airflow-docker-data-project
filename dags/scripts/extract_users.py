from pathlib import Path
import requests
import json
import os

def run():
    API_URL = os.environ["USERS_API_URL"]
    RAW_PATH = Path(os.environ["RAW_DATA_PATH"])

    if not API_URL or not RAW_PATH:
        raise ValueError("Variáveis de ambiente não configuradas corretamente.")

    response = requests.get(API_URL)
    response.raise_for_status()

    data = response.json()

    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(RAW_PATH, "w") as f:
        json.dump(data, f, indent=4)

    print("Dados extraídos com sucesso.")

if __name__ == "__main__":
    run()
