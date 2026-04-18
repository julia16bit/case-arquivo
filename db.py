import os
import logging
import psycopg2
from psycopg2.extras import Json, execute_values

from colored_logging import setup_colored_logging

logger = setup_colored_logging("db")

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
            payload_hash TEXT GENERATED ALWAYS AS (md5(payload::text)) STORED,
            carregado_em TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            CONSTRAINT unique_fonte_payload_hash UNIQUE (fonte, payload_hash)
        )
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'dados_processados' AND column_name = 'payload_hash'
                        ) THEN
                            ALTER TABLE dados_processados
                            ADD COLUMN payload_hash TEXT GENERATED ALWAYS AS (md5(payload::text)) STORED;
                        END IF;

                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = 'unique_fonte_payload_hash'
                        ) THEN
                            DELETE FROM dados_processados a
                            USING dados_processados b
                            WHERE a.id > b.id
                              AND a.fonte = b.fonte
                              AND md5(a.payload::text) = md5(b.payload::text);

                            ALTER TABLE dados_processados
                            ADD CONSTRAINT unique_fonte_payload_hash UNIQUE (fonte, payload_hash);
                        END IF;
                    END
                    $$;
                """)
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
    sql = (
        "INSERT INTO dados_processados (fonte, payload) VALUES %s "
        "ON CONFLICT ON CONSTRAINT unique_fonte_payload_hash DO NOTHING"
    )
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values)
                conn.commit()
        # Log seguro: sem revelar dados sensíveis
        logger.info("Inseridas %s linhas no banco (origem: %s)", len(rows), source_file)
    except Exception as exc:
        logger.exception("Erro ao inserir linhas (arquivo: %s)", source_file)
        raise
