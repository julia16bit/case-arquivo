<h2 align="center">🟦 Pipeline Manager</h2>
<p align="center">Pipeline de dados em duas etapas com foco em confiabilidade, segurança e observabilidade. O projeto valida e limpa dados na etapa A, transforma e insere no PostgreSQL na etapa B, gerando um fluxo de dados em lote seguro e repetível.</p>
  
<p align="center"> 
  <img alt="Python" src="https://img.shields.io/badge/Python-2E3849?style=for-the-badge&logo=python&logoColor=white&color=2E3849&labelColor=2E3849"/>
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-2E3849?style=for-the-badge&logo=postgresql&logoColor=white&color=2E3849&labelColor=2E3849"/>
  <img alt="psycopg2" src="https://img.shields.io/badge/psycopg2-2E3849?style=for-the-badge&logo=postgresql&logoColor=white&color=2E3849&labelColor=2E3849"/>
  <img alt="CSV" src="https://img.shields.io/badge/CSV-2E3849?style=for-the-badge&logo=csv&logoColor=white&color=2E3849&labelColor=2E3849"/>
  <img alt="Logging" src="https://img.shields.io/badge/Logging-2E3849?style=for-the-badge&logo=logstash&logoColor=white&color=2E3849&labelColor=2E3849"/>
</p>

## Descrição

Projeto de pipeline de dados em Python que processa arquivos CSV em lote usando duas etapas:

- **Worker A**: valida e limpa dados de `entrada/`, produz arquivos em `processado_a/` e move os originais para `pronto/`.
- **Worker B**: transforma os dados de `processado_a/`, normaliza tipos e insere registros em PostgreSQL.

O fluxo evita reprocessar arquivos e, agora, também previne inserções duplicadas no banco quando o mesmo conteúdo é reenviado.
O pipeline foi projetado para evolução futura com múltiplos workers e integração com filas para maior escalabilidade.

## Fluxo de Dados

```
entrada/ 
   ↓
[Worker A: Validação + Limpeza]
   ├─→ processado_a/ (arquivo limpo)
   └─→ pronto/ (original)
       ↓
   [Worker B: Transformação + Inserção]
       ├─→ PostgreSQL (JSON payload)
       └─→ pronto/ (arquivo finalizado)
```

**Etapa A (Worker A):**
1. Lê arquivo CSV em `entrada/`
2. Valida tamanho, linhas e schema
3. Remove linhas vazias, normaliza valores e detecta fórmulas perigosas
4. Salva cópia limpa em `processado_a/` via arquivo `.tmp`
5. Move original para `pronto/`

**Etapa B (Worker B):**
1. Lê arquivo limpo de `processado_a/`
2. Converte tipos (`int`, `float`, `bool`, `str`, `null`)
3. Insere no banco como `fonte` + `payload JSON`
4. Usa restrição única para evitar duplicatas
5. Move arquivo finalizado para `pronto/`

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
- Usa restrição única no banco para evitar inserções duplicadas de mesmo payload e fonte
- Move o arquivo processado para `pronto/`

### ✅ Robustez
- Evita reprocessar arquivos ao mover os arquivos finalizados para `pronto/`
- Previne inserções duplicadas no banco usando `ON CONFLICT DO NOTHING` e hash de payload
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
### Arquivos Principais

```bash
.
├── worker_a.py              # Validação, limpeza e normalização de CSV
├── worker_b.py              # Transformação de tipos e inserção no banco
├── db.py                    # Conexão PostgreSQL e bulk insert com deduplicação
├── validation.py            # Validações de segurança (tamanho, linhas, fórmulas, sanitização)
├── colored_logging.py       # Sistema de logs colorido ANSI
├── metrics.py               # Rastreamento de métricas e performance
├── metrics_viewer.py        # Visualizador de métricas do pipeline
├── testes/                  # Scripts para gerar dados de teste
│   ├── generate_sample_input.py         # Gera CSV com 10.000 linhas
│   ├── generate_test_discarded_lines.py # Gera CSV com linhas descartadas
│   └── demo_security.py                 # Demonstração de controles de segurança
├── README.md                # Este arquivo
├── SECURITY.md              # Documentação completa de segurança
└── LICENSE                  # Licença do projeto
```

### Diretórios de Dados

```bash
entrada/        # Novos arquivos CSV chegam aqui
processado_a/   # Arquivos após Worker A (limpos e validados)
pronto/         # Arquivos finalizados após Worker B
```

### Características por Arquivo

| Arquivo | Responsabilidade |
|---------|------------------|
| `worker_a.py` | Valida, limpa, normaliza e formata CSV; move original para `pronto/` |
| `worker_b.py` | Transforma tipos, insere no PostgreSQL com deduplicação; finaliza arquivo |
| `db.py` | Gerencia conexão, cria tabela com restrição única e insere em lote |
| `validation.py` | Validações de tamanho (50 MB), linhas (100k), schema, fórmulas Excel e sanitização |
| `colored_logging.py` | Logs ANSI coloridos para melhor observabilidade |
| `metrics.py` | Rastreia tempo de processamento, linhas, erros por arquivo e agregações |
| `metrics_viewer.py` | Exibe métricas em formato legível (sumário e detalhado) |
| `testes/generate_sample_input.py` | Gera CSV com 10.000 linhas para teste básico |
| `testes/generate_test_discarded_lines.py` | Gera CSV com linhas que serão descartadas |
| `testes/demo_security.py` | Exemplos e testes de segurança |

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

## 📊 Observabilidade e Métricas

O pipeline rastreia automaticamente as seguintes métricas em `metrics.json`:

### Por Arquivo
- ⏱️ Tempo de processamento (Worker A e Worker B)
- ✅ Número de linhas processadas
- ❌ Número de linhas descartadas
- ⚠️ Erros e exceções capturadas

### Resumo Geral
- 📈 Total de arquivos processados
- 📊 Linhas médias por arquivo
- ⏱️ Tempo médio de processamento
- 📉 Taxa de processamento (arquivos/minuto)
- 🔴 Total de erros registrados
- 💾 As métricas são persistidas localmente em `metrics.json` para análise histórica simples.

### Visualizar Métricas
```bash
python metrics_viewer.py
```

## Exemplos de Uso

### Gerar dados de teste
```bash
python testes/generate_sample_input.py
```

### Testar descarte de linhas
```bash
python testes/generate_test_discarded_lines.py
python worker_a.py
python worker_b.py
python metrics_viewer.py
```

Você verá linhas sendo descartadas quando todas as colunas contêm fórmulas perigosas, caracteres inválidos ou estão vazias.

### Demonstrar controles de segurança
```bash
python testes/demo_security.py
python worker_a.py
python worker_b.py
```

### Visualizar métricas
```bash
python metrics_viewer.py
```

## 🔐 Segurança

Veja [SECURITY.md](SECURITY.md) para documentação completa sobre:
- Validação de CSV (limite de tamanho, detecção de fórmulas, sanitização)
- Arquivos temporários (.tmp) para evitar leitura parcial
- Prevenção de reprocessamento
- Logs seguros (sem dados sensíveis)
- Controle de acesso e boas práticas

> 📌 O projeto está pensado para ser simples: sem frameworks complexos, com processamento em lote e lógica clara de pastas de entrada, saída intermediária e pronto.
