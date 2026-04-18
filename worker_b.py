import csv
import logging
import os
import re
import time
from pathlib import Path

from db import create_table, insert_rows
from validation import sanitize_value
from colored_logging import setup_colored_logging

INPUT_DIR = Path("processado_a")
DONE_DIR = Path("pronto")
POLL_SECONDS = int(os.getenv("WORKER_B_POLL_SECONDS", "30"))

# Usa logging colorido
logger = setup_colored_logging("worker_b")


def ensure_directories():
    """Cria as pastas necessárias: processado_a e pronto."""
    for path in (INPUT_DIR, DONE_DIR):
        path.mkdir(parents=True, exist_ok=True)
    logger.info("Diretórios verificados/criados")


def normalize_text(value):
    """Normaliza o texto: remove espaços extras e converte vazios para None."""
    if value is None:
        return None
    text = value.strip()
    text = re.sub(r"\s+", " ", text)
    # Sanitizar valor perigoso
    safe_text = sanitize_value(text)
    return safe_text if safe_text != "" else None


def convert_value(value):
    """Converte o valor para o tipo apropriado: booleano, inteiro, float ou string."""
    text = normalize_text(value)
    if text is None:
        return None

    lower = text.lower()
    # Conversão de booleano
    if lower in ("true", "false", "sim", "nao", "não"):
        return lower in ("true", "sim")
    # Conversão de inteiro
    if re.fullmatch(r"-?\d+", text):
        try:
            return int(text)
        except ValueError:
            pass
    # Conversão de float
    if re.fullmatch(r"-?\d+\.\d+", text):
        try:
            return float(text)
        except ValueError:
            pass
    # Mantém como string
    return text


def transform_row(row):
    """Aplica transformação em cada linha e descarta linhas totalmente vazias."""
    transformed = {key: convert_value(value) for key, value in row.items()}
    if all(value is None for value in transformed.values()):
        return None
    return transformed


def safe_move(src, dest_dir):
    """Move arquivo com nome único para evitar conflitos."""
    dest = dest_dir / src.name
    if dest.exists():
        dest = dest_dir / f"{src.stem}_{int(time.time())}{src.suffix}"
    src.rename(dest)
    logger.info("Arquivo movido: %s -> %s", src.name, dest.name)
    return dest


def process_file(file_path):
    """Processa arquivo CSV: transforma dados, insere em BD e move para pronto."""
    logger.info("Iniciando transformação de %s", file_path.name)
    
    try:
        # 1. Leitura e transformação
        with file_path.open("r", newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            rows = [transform_row(row) for row in reader]
            rows = [row for row in rows if row is not None]

        if not rows:
            logger.warning("Arquivo %s não contém linhas válidas para inserir", file_path.name)
            safe_move(file_path, DONE_DIR)
            return

        # 2. Criar tabela se necessário
        create_table()
        
        # 3. Inserir dados no banco (operação segura com logs)
        insert_rows(rows, file_path.name)
        
        # 4. Mover para pronto
        safe_move(file_path, DONE_DIR)
        logger.info("Arquivo %s processado com sucesso (%s linhas inseridas)", file_path.name, len(rows))
        
    except Exception as exc:
        logger.exception("Erro ao processar %s: %s", file_path.name, exc)
        # Mover arquivo com erro para pronto para não ficar preso
        safe_move(file_path, DONE_DIR)


def process_all_files():
    """Processa todos os arquivos CSV em processado_a/."""
    files = list(INPUT_DIR.glob("*.csv"))
    if files:
        logger.info("Encontrados %s arquivo(s) para processar", len(files))
    for file_path in sorted(files):
        process_file(file_path)


def main():
    """Função principal: inicia o processamento e, opcionalmente, modo contínuo."""
    ensure_directories()
    process_all_files()
    if os.getenv("WORKER_B_LOOP", "false").lower() == "true":
        logger.info("Modo contínuo ativado. Verificando novos arquivos a cada %s segundos.", POLL_SECONDS)
        while True:
            time.sleep(POLL_SECONDS)
            process_all_files()


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
