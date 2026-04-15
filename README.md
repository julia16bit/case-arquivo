# case-arquivo
Pipeline de processamento de arquivos em Python com arquitetura em duas etapas.

Este projeto demonstra um fluxo simples para processar arquivos CSV em lote:

1. `worker_a.py`
   - lê arquivos CSV de `entrada/`
   - valida e limpa os dados
   - salva os arquivos limpos em `processado_a/`
   - move os arquivos originais para `pronto/`

2. `worker_b.py`
   - lê os CSV de `processado_a/`
   - transforma e normaliza os valores
   - insere os dados no PostgreSQL
   - move os arquivos processados para `pronto/`

## Estrutura do projeto

- `worker_a.py`: validação e limpeza dos dados
- `worker_b.py`: transformação e inserção no PostgreSQL
- `db.py`: conexão e operações básicas no banco
- `entrada/`: pasta de arquivos CSV de entrada
- `processado_a/`: pasta intermediária após a etapa A
- `pronto/`: pasta de arquivos já finalizados
- `entrada/sample_input.csv`: exemplo de arquivo CSV de teste

## Requisitos

- Python 3.8+
- PostgreSQL acessível via rede/local
- `psycopg2-binary`

Instale as dependências:

```bash
pip install psycopg2-binary
```

## Configuração do banco de dados

Defina as variáveis de ambiente do PostgreSQL antes de executar o worker B:

```bash
set DB_HOST=localhost
set DB_PORT=5432
set DB_NAME=pipeline_db
set DB_USER=postgres
set DB_PASSWORD=postgres
```

## Execução

1. Coloque `csv` em `entrada/` ou use o arquivo de exemplo `entrada/sample_input.csv`.
2. Execute o worker de limpeza:

```bash
python worker_a.py
```

3. Execute o worker de transformação e inserção:

```bash
python worker_b.py
```

## Execução contínua opcional

Para ativar o modo de verificação contínua, defina a variável de ambiente `WORKER_A_LOOP` ou `WORKER_B_LOOP` como `true`.

```bash
set WORKER_A_LOOP=true
set WORKER_B_LOOP=true
```

Os workers então verificarão novos arquivos a cada 30 segundos por padrão.
