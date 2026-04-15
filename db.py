import os
import logging
import psycopg2
from psycopg2.extras import Json, execute_values

logger = logging.getLogger("db")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Função para obter a configuração do banco de dados a partir de variáveis de ambiente, com valores padrão
def get_db_config():
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "dbname": os.getenv("DB_NAME", "pipeline_db"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    }

# Função para criar uma conexão com o banco de dados usando a configuração obtida
def get_connection():
    config = get_db_config()
    logger.debug("Conectando ao banco com %s", config)
    return psycopg2.connect(**config)

# Função para criar a tabela de dados processados, se ela ainda não existir
def create_table():
    sql = """
        CREATE TABLE IF NOT EXISTS dados_processados (
            id SERIAL PRIMARY KEY,
            fonte TEXT NOT NULL,
            payload JSONB NOT NULL,
            carregado_em TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        )
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        logger.info("Tabela dados_processados verificada/criada com sucesso")
    except Exception as exc:
        logger.exception("Erro ao criar tabela: %s", exc)
        raise


# Função para inserir linhas na tabela de dados processados, recebendo uma lista de dicionários e o nome do arquivo de origem para referência
def insert_rows(rows, source_file):
    if not rows:
        logger.info("Nenhuma linha para inserir no banco")
        return

    values = [(source_file, Json(row)) for row in rows]
    sql = "INSERT INTO dados_processados (fonte, payload) VALUES %s"
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values)
                conn.commit()
        logger.info("Inseridas %s linhas no banco", len(rows))
    except Exception as exc:
        logger.exception("Erro ao inserir linhas: %s", exc)
        raise
