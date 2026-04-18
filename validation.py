import re
import logging
from pathlib import Path

from colored_logging import setup_colored_logging

logger = setup_colored_logging("validation")

# Configurações de segurança
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
MAX_ROWS = 100000
DANGEROUS_PATTERNS = [
    r'^\=',  # Excel formula =
    r'^\@',  # Excel macro @
    r'^\+',  # Excel formula +
    r'^\-',  # Excel formula -
]
# Permite acentos e caracteres especiais comuns (UTF-8)
# Bloqueia apenas: <, >, {, }, [, ], |, \, ^, ~, `, etc
ALLOWED_CHARS = re.compile(r'^[^<>\{\}\[\]\|\\\^~`]*$', re.UNICODE)


def validate_file_size(file_path):
    """Valida se o arquivo está dentro do limite de tamanho."""
    size = file_path.stat().st_size
    if size > MAX_FILE_SIZE:
        logger.error("Arquivo %s excede limite de tamanho (%s > %s)", file_path.name, size, MAX_FILE_SIZE)
        return False
    return True


def validate_schema(fieldnames):
    """Valida se o schema do CSV é válido (não vazio)."""
    if not fieldnames or len(fieldnames) == 0:
        logger.error("CSV sem cabeçalho válido")
        return False
    return True


def is_dangerous(value):
    """Verifica se a string contém padrões perigosos (fórmulas Excel, etc)."""
    if value is None:
        return False
    value_str = str(value).strip()
    for pattern in DANGEROUS_PATTERNS:
        if re.match(pattern, value_str):
            return True
    return False


def sanitize_value(value):
    """Remove ou marca valores perigosos."""
    if value is None:
        return None
    value_str = str(value).strip()
    if is_dangerous(value_str):
        logger.warning("Valor perigoso detectado e removido: %s", value_str[:20])
        return None
    # Remover caracteres muito estranhos
    if not ALLOWED_CHARS.match(value_str):
        logger.warning("Caracteres inválidos detectados em: %s", value_str[:20])
        return None
    return value_str


def validate_row_count(file_path):
    """Valida se o arquivo não excede o número máximo de linhas."""
    import csv
    with file_path.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = sum(1 for _ in reader)
        if count > MAX_ROWS:
            logger.error("Arquivo %s excede limite de linhas (%s > %s)", file_path.name, count, MAX_ROWS)
            return False
    return True


def validate_csv_file(file_path):
    """Validação completa do arquivo CSV."""
    logger.info("Validando arquivo %s", file_path.name)
    
    # 1. Validar tamanho
    if not validate_file_size(file_path):
        return False
    
    # 2. Validar contagem de linhas
    if not validate_row_count(file_path):
        return False
    
    # 3. Validar schema
    import csv
    try:
        with file_path.open('r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not validate_schema(reader.fieldnames):
                return False
    except Exception as exc:
        logger.error("Erro ao ler CSV %s: %s", file_path.name, exc)
        return False
    
    logger.info("Arquivo %s passou na validação", file_path.name)
    return True
