import csv
import logging
import os
import re
import time
from pathlib import Path

from db import create_table, insert_rows

INPUT_DIR = Path("processado_a")
DONE_DIR = Path("pronto")
POLL_SECONDS = int(os.getenv("WORKER_B_POLL_SECONDS", "30")) # Tempo de espera entre verificações no modo contínuo, em segundos

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s") # Configura o logging para exibir informações de tempo, nível e mensagem
logger = logging.getLogger("worker_b")

# Essa função garante que as pastas existam antes de processar, evitando erros de diretório não encontrado
def ensure_directories():
    for path in (INPUT_DIR, DONE_DIR):
        path.mkdir(parents=True, exist_ok=True)

# Normaliza o texto, removendo espaços extras e convertendo strings vazias para None
def normalize_text(value):
    if value is None:
        return None
    text = value.strip()
    text = re.sub(r"\s+", " ", text)
    return text if text != "" else None

# Converte o valor para o tipo apropriado (booleano, inteiro, float ou string)
def convert_value(value):
    text = normalize_text(value)
    if text is None:
        return None

    lower = text.lower()
    if lower in ("true", "false", "sim", "nao", "não"):
        return lower in ("true", "sim")
    if re.fullmatch(r"-?\d+", text):
        try:
            return int(text)
        except ValueError:
            pass
    if re.fullmatch(r"-?\d+\.\d+", text):
        try:
            return float(text)
        except ValueError:
            pass
    return text

# Aplica a transformação em cada linha e descarta linhas totalmente vazias
def transform_row(row):
    transformed = {key: convert_value(value) for key, value in row.items()}
    if all(value is None for value in transformed.values()):
        return None
    return transformed

# Move o arquivo para a pasta de destino, renomeando se já existir um arquivo com o mesmo nome
def safe_move(src, dest_dir):
    dest = dest_dir / src.name
    if dest.exists():
        dest = dest_dir / f"{src.stem}_{int(time.time())}{src.suffix}"
    src.rename(dest)
    return dest

# Processa um arquivo CSV, transformando os dados e inserindo no banco, ou movendo para pronto se não tiver linhas válidas
def process_file(file_path):
    logger.info("Transformando arquivo %s", file_path.name)
    try:
        with file_path.open("r", newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            rows = [transform_row(row) for row in reader]
            rows = [row for row in rows if row is not None]

        if not rows:
            logger.warning("Arquivo %s não contém linhas válidas para inserir", file_path.name)
            safe_move(file_path, DONE_DIR)
            return

        create_table()
        insert_rows(rows, file_path.name)
        safe_move(file_path, DONE_DIR)
        logger.info("Arquivo %s inserido no banco e movido para pronto", file_path.name)
    except Exception as exc:
        logger.exception("Erro ao transformar %s: %s", file_path.name, exc)

# Percorre todos os arquivos CSV da pasta de entrada e processa um por um
def process_all_files():
    for file_path in sorted(INPUT_DIR.glob("*.csv")):
        process_file(file_path)

# Função principal que inicia o processo e, se configurado, mantém o worker rodando continuamente para verificar novos arquivos
def main():
    ensure_directories()
    process_all_files()
    if os.getenv("WORKER_B_LOOP", "false").lower() == "true": # Verifica se o modo contínuo está ativado pela variável de ambiente
        logger.info("Modo contínuo ativado para Worker B. Verificando novos arquivos a cada %s segundos.", POLL_SECONDS)
        while True:
            time.sleep(POLL_SECONDS)
            process_all_files()


if __name__ == "__main__":
    main()
