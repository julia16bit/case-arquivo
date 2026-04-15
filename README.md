<h2 align="center">🟦 Pipeline CSV Manager</h2>
<p align="center">API simples de processamento em duas etapas para arquivos CSV. O projeto valida e limpa dados na etapa A, transforma e insere no PostgreSQL na etapa B, gerando um fluxo de dados em lote seguro e repetível.</p>
  
<p align="center"> 
  <img alt="Python" src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white&color=3776AB&labelColor=3776AB" />
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white&color=316192&labelColor=316192" />
  <img alt="psycopg2" src="https://img.shields.io/badge/psycopg2-2E3849?style=for-the-badge&logo=postgresql&logoColor=white&color=2E3849&labelColor=2E3849" />
  <img alt="CSV" src="https://img.shields.io/badge/CSV-2E3849?style=for-the-badge&logo=csv&logoColor=white&color=2E3849&labelColor=2E3849" />
  <img alt="Logging" src="https://img.shields.io/badge/Logging-2E3849?style=for-the-badge&logo=logstash&logoColor=white&color=2E3849&labelColor=2E3849" />
</p>

## Descrição

Projeto de pipeline de dados em Python que processa arquivos CSV em lote usando duas etapas:

- **Worker A**: valida e limpa dados de `entrada/`, produz arquivos em `processado_a/` e move os originais para `pronto/`.
- **Worker B**: transforma os dados de `processado_a/`, normaliza tipos e insere registros em PostgreSQL.

O fluxo evita reprocessar arquivos e mantém o código simples, legível e fácil de entender.

## Tecnologias Utilizadas

| Categoria                                   | Tecnologia                             | Descrição                                    |
| ------------------------------------------- | -------------------------------------- | -------------------------------------------- |
| ⚡ Runtime                                   | Python 3.8+                            | Execução do pipeline                         |
| 🗄️ Banco de Dados                            | PostgreSQL                             | Armazenamento final dos dados                |
| 🔌 Conexão                                  | psycopg2                               | Driver para PostgreSQL                       |
| 🧹 Processamento                              | CSV                                    | Leitura e escrita de arquivos CSV            |
| 🧾 Logs                                      | logging                                | Registro de execução e erros                 |
| 🧪 Validação                                 | try/except                             | Tratamento de erros simples                  |
| 📁 Estrutura                                | Pastas locais                          | `entrada/`, `processado_a/`, `pronto/`       |

## Funcionalidades

### 🧼 Worker A - Validação e Limpeza
- Lê todos os arquivos CSV em `entrada/`
- Remove linhas vazias
- Normaliza valores em branco para `null`
- Preserva cabeçalhos e salva arquivos limpos em `processado_a/`
- Move o arquivo original para `pronto/`

### 🔄 Worker B - Transformação e Inserção
- Lê CSV gerados em `processado_a/`
- Normaliza texto e converte tipos (`int`, `float`, `bool`, `null`)
- Insere os dados no PostgreSQL em lote
- Move o arquivo processado para `pronto/`

### ✅ Robustez
- Evita reprocessar arquivos ao mover os arquivos finalizados para `pronto/`
- Usa logs básicos para acompanhar processamento
- Implementa tratamento de erros com `try/except`

## Como executar o projeto

### ⚠️ Pré-requisitos
- Python 3.8+
- PostgreSQL instalado e acessível
- Dependência: `psycopg2-binary`

### Passos
```bash
cd C:\Users\Administrador\Documents\github\case-arquivo
python -m pip install psycopg2-binary
```

Crie as variáveis de ambiente do banco (PowerShell):
```powershell
$env:DB_HOST = "localhost"
$env:DB_PORT = "5432"
$env:DB_NAME = "pipeline_db"
$env:DB_USER = "postgres"
$env:DB_PASSWORD = "postgres"
```

### Executar os workers

```bash
python worker_a.py
python worker_b.py
```

### Modo contínuo opcional

```powershell
$env:WORKER_A_LOOP = "true"
$env:WORKER_B_LOOP = "true"
```

Ambos os workers então irão verificar novos arquivos a cada 30 segundos.

## Estrutura do Projeto

```bash
.
├── db.py              # Conexão e inserção no PostgreSQL
├── worker_a.py        # Validação e limpeza de CSV
├── worker_b.py        # Transformação e inserção em lote
├── generate_sample_input.py # Gera exemplo CSV com 10.000 linhas
├── entrada/           # Arquivos CSV de entrada
├── processado_a/      # Saída da etapa A
└── pronto/            # Arquivos processados e finalizados
```

## Exemplo de Arquivo de Entrada

O arquivo de exemplo `entrada/sample_input.csv` contém nomes, idades e salários. Ele demonstra o fluxo de limpeza e transformação do pipeline.

## Verificação no Banco

Use o `psql` ou Python para confirmar que os dados foram inseridos:

```sql
SELECT count(*) FROM dados_processados;
SELECT fonte, payload FROM dados_processados LIMIT 10;
```

> 📌 O projeto está pensado para ser simples: sem frameworks complexos, com processamento em lote e lógica clara de pastas de entrada, saída intermediária e pronto.
