"""
Módulo de logging customizado com cores ANSI.
Use para adicionar cores aos logs de WARNING, ERROR e INFO.
"""

import logging
import sys

# Cores ANSI
RESET = '\033[0m'
BOLD = '\033[1m'
DIM = '\033[2m'

# Cores de nível
INFO_COLOR = '\033[36m'      # Cyan
WARNING_COLOR = '\033[33m'   # Yellow
ERROR_COLOR = '\033[31m'     # Red
DEBUG_COLOR = '\033[35m'     # Magenta
SUCCESS_COLOR = '\033[32m'   # Green


class ColoredFormatter(logging.Formatter):
    """Formatter que adiciona cores aos logs."""
    
    COLORS = {
        'DEBUG': DEBUG_COLOR,
        'INFO': INFO_COLOR,
        'WARNING': WARNING_COLOR,
        'ERROR': ERROR_COLOR,
        'CRITICAL': ERROR_COLOR,
    }
    
    def format(self, record):
        # Adiciona cor ao nível do log
        levelname = record.levelname
        color = self.COLORS.get(levelname, RESET)
        
        # Formata com cor
        record.levelname = f"{color}{BOLD}{levelname}{RESET}"
        
        # Formata a mensagem
        result = super().format(record)
        return result


def setup_colored_logging(logger_name, level=logging.INFO):
    """
    Configura um logger com saída colorida.
    
    Uso:
        logger = setup_colored_logging("meu_app")
        logger.info("Mensagem")
        logger.warning("Aviso")
        logger.error("Erro")
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    # Remove handlers existentes
    logger.handlers.clear()
    
    # Cria handler para console
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    
    # Cria formatter colorido
    formatter = ColoredFormatter(
        f"%(asctime)s {DIM}%(levelname)s{RESET} %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger
