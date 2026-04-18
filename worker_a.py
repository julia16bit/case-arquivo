import csv
import logging
import os
import time
from pathlib import Path

from validation import validate_csv_file, sanitize_value
from colored_logging import setup_colored_logging

INPUT_DIR = Path("entrada")
STEP_A_DIR = Path("processado_a")
DONE_DIR = Path("pronto")
POLL_SECONDS = int(os.getenv("WORKER_A_POLL_SECONDS", "30"))

# Usa logging colorido
logger = setup_colored_logging("worker_a")


def ensure_directories():
    """Cria as pastas necessárias: entrada, processado_a e pronto."""
    for path in (INPUT_DIR, STEP_A_DIR, DONE_DIR):
        path.mkdir(parents=True, exist_ok=True)
    logger.info("Diretórios verificados/criados")


def normalize_value(value):
    """Normaliza valor: remove espaços, sanitiza e converte vazios para None."""
    if value is None:
        return None
    value = value.strip()
    # Sanitizar valor perigoso (fórmulas Excel, etc)
    safe_value = sanitize_value(value)
    return safe_value if safe_value != "" else None


def clean_row(row):
    """Remove valores vazios e retorna None se linha inteira estiver vazia."""
    cleaned = {key: normalize_value(value) for key, value in row.items()}
    if all(value is None for value in cleaned.values()):
        return None
    return cleaned


def safe_move(src, dest_dir):
    """Move arquivo com nome único para evitar conflitos."""
    dest = dest_dir / src.name
    if dest.exists():
        dest = dest_dir / f"{src.stem}_{int(time.time())}{src.suffix}"
    src.rename(dest)
    logger.info("Arquivo movido: %s -> %s", src.name, dest.name)
    return dest


def process_file(file_path):
    """Processa arquivo CSV: valida, limpa e salva em arquivo temporário, depois move."""
    logger.info("Iniciando processamento de %s", file_path.name)
    
    # 1. Validação de segurança
    if not validate_csv_file(file_path):
        logger.error("Arquivo %s falhou na validação, será descartado", file_path.name)
        safe_move(file_path, DONE_DIR)
        return
    
    try:
        # 2. Leitura e limpeza
        with file_path.open("r", newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            fieldnames = reader.fieldnames or []
            cleaned_rows = [clean_row(row) for row in reader]
            cleaned_rows = [row for row in cleaned_rows if row is not None]

        if not fieldnames:
            logger.warning("Arquivo %s não possui cabeçalho válido", file_path.name)
            safe_move(file_path, DONE_DIR)
            return

        # 3. Usar arquivo temporário durante escrita
        output_path = STEP_A_DIR / file_path.name
        temp_path = output_path.with_suffix(".tmp")
        
        with temp_path.open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_rows)
        
        # 4. Renomear de .tmp para .csv (operação atômica)
        temp_path.rename(output_path)
        logger.info("Arquivo limpo e salvo: %s (%s linhas)", output_path.name, len(cleaned_rows))
        
        # 5. Mover original para pronto
        safe_move(file_path, DONE_DIR)
        
    except Exception as exc:
        logger.exception("Erro ao processar %s: %s", file_path.name, exc)
        safe_move(file_path, DONE_DIR)


def process_all_files():
    """Processa todos os arquivos CSV em entrada/."""
    files = list(INPUT_DIR.glob("*.csv"))
    if files:
        logger.info("Encontrados %s arquivo(s) para processar", len(files))
    for file_path in sorted(files):
        process_file(file_path)


def main():
    """Função principal: inicia o processamento e, opcionalmente, modo contínuo."""
    ensure_directories()
    process_all_files()
    if os.getenv("WORKER_A_LOOP", "false").lower() == "true":
        logger.info("Modo contínuo ativado. Verificando novos arquivos a cada %s segundos.", POLL_SECONDS)
        while True:
            time.sleep(POLL_SECONDS)
            process_all_files()


if __name__ == "__main__":
    main()
