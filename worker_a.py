import csv
import logging
import os
import time
from pathlib import Path

INPUT_DIR = Path("entrada")
STEP_A_DIR = Path("processado_a")
DONE_DIR = Path("pronto")
POLL_SECONDS = int(os.getenv("WORKER_A_POLL_SECONDS", "30")) # Tempo de espera entre verificações no modo contínuo, em segundos

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s") # Configura o logging para exibir informações de tempo, nível e mensagem
logger = logging.getLogger("worker_a")

# Essa função garante que as pastas existam antes de processar, evitando erros de diretório não encontrado
def ensure_directories():
    for path in (INPUT_DIR, STEP_A_DIR, DONE_DIR):
        path.mkdir(parents=True, exist_ok=True)

# Normaliza o valor, removendo espaços e convertendo strings vazias para None
def normalize_value(value):
    if value is None:
        return None
    value = value.strip()
    return value if value != "" else None

# Aplico a limpeza em cada linha e descarto linhas totalmente vazias
def clean_row(row):
    cleaned = {key: normalize_value(value) for key, value in row.items()}
    if all(value is None for value in cleaned.values()):
        return None
    return cleaned

# Move o arquivo para a pasta de destino, renomeando se já existir um arquivo com o mesmo nome
def safe_move(src, dest_dir):
    dest = dest_dir / src.name
    if dest.exists():
        dest = dest_dir / f"{src.stem}_{int(time.time())}{src.suffix}"
    src.rename(dest)
    return dest

# Processa um arquivo CSV, limpando os dados e salvando o resultado, ou movendo para pronto se não tiver cabeçalho
def process_file(file_path):
    logger.info("Processando arquivo %s", file_path.name)
    try:
        with file_path.open("r", newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            fieldnames = reader.fieldnames or []
            cleaned_rows = [clean_row(row) for row in reader]
            cleaned_rows = [row for row in cleaned_rows if row is not None]

        if not fieldnames:
            logger.warning("Arquivo %s não possui cabeçalho, será movido para pronto", file_path.name)
            safe_move(file_path, DONE_DIR)
            return

        output_path = STEP_A_DIR / file_path.name
        with output_path.open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_rows)

        safe_move(file_path, DONE_DIR)
        logger.info("Arquivo %s limpo e salvo em %s", file_path.name, output_path)
    except Exception as exc:
        logger.exception("Erro ao processar %s: %s", file_path.name, exc)

# Percorre todos os arquivos CSV da pasta de entrada e processa um por um
def process_all_files():
    for file_path in sorted(INPUT_DIR.glob("*.csv")):
        process_file(file_path)

# Função principal que inicia o processo e, se configurado, mantém o worker rodando continuamente para verificar novos arquivos
def main():
    ensure_directories()
    process_all_files()
    if os.getenv("WORKER_A_LOOP", "false").lower() == "true": # Verifica se o modo contínuo está ativado pela variável de ambiente
        logger.info("Modo contínuo ativado para Worker A. Verificando novos arquivos a cada %s segundos.", POLL_SECONDS)
        while True:
            time.sleep(POLL_SECONDS)
            process_all_files()


if __name__ == "__main__":
    main()
