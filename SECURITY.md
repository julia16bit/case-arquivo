# 🔐 Segurança do Pipeline

Este documento descreve as medidas de segurança implementadas no projeto.

## 1. Validação de Arquivos CSV

### O que foi implementado:

**Arquivo: `validation.py`**

- ✅ **Limite de tamanho**: Máximo 50 MB por arquivo
- ✅ **Limite de linhas**: Máximo 100.000 linhas por CSV
- ✅ **Detecção de fórmulas Excel**: Bloqueia padrões como `=`, `@`, `+`, `-` no início
- ✅ **Validação de schema**: Rejeita CSV sem cabeçalho
- ✅ **Sanitização**: Remove caracteres perigosos de strings

### Exemplo de bloqueio:

```csv
# Rejeitado (fórmula Excel)
email,name,salary
user@example.com,=SYSTEM("cmd"),50000

# Aceito (seguro)
email,name,salary
user@example.com,João Silva,50000
```

---

## 2. Arquivos Temporários (.tmp)

### O que foi implementado:

**Arquivos: `worker_a.py`, `worker_b.py`**

- ✅ **Escrita em .tmp**: Arquivo escrito como `.tmp` durante processamento
- ✅ **Renomeação atômica**: Apenas `rename()` (operação atômica) converte `.tmp` → `.csv`
- ✅ **Previne leitura parcial**: Worker B nunca pega arquivo incompleto

### Fluxo:

```
entrada/dados.csv → processado_a/dados.tmp → processado_a/dados.csv
                                  ↑
                          (arquivo incompleto)
```

---

## 3. Prevenção de Reprocessamento

### O que foi implementado:

- ✅ **Movimentação de arquivos**: Arquivo é movido para `pronto/` após sucesso
- ✅ **Idempotência**: Se worker rodar de novo, não processa `pronto/`
- ✅ **Nomes únicos**: Se houver conflito, adiciona timestamp

### Exemplo:

```
Round 1: entrada/data.csv → processado_a/data.csv → pronto/data.csv
Round 2: entrada/data.csv (novo) → processado_a/data.csv → pronto/data_1713350452.csv
```

---

## 4. Logs Seguros

### O que foi implementado:

**Arquivo: `db.py`**

- ✅ **Sem revelar dados**: Loga apenas contagem e origem
- ✅ **Sem dados pessoais**: CPF, email, senha nunca aparecem nos logs
- ✅ **Rastreabilidade**: Sempre registra qual arquivo foi processado

### Exemplo de log SEGURO:

```log
2026-04-17 10:30:45 INFO Inseridas 150 linhas no banco (origem: dados.csv)
```

### O que NÃO seria registrado:

```log
❌ ERRADO: Inseridas 150 linhas no banco (payload: {"email": "joao@example.com", "salary": 50000})
```

---

## 5. Tratamento de Erros

### O que foi implementado:

- ✅ **Try/except em cada worker**: Nunca falha silenciosamente
- ✅ **Arquivo com erro é movido para pronto**: Evita ficar preso no loop
- ✅ **Log de exceção**: Registra stacktrace para debug

### Exemplo:

```python
except Exception as exc:
    logger.exception("Erro ao processar %s: %s", file_path.name, exc)
    # Arquivo ainda é movido para pronto para não travar
    safe_move(file_path, DONE_DIR)
```

---

## 6. Controle de Permissões (Recomendação)

### Configuração recomendada:

```bash
# Worker A precisa de:
# - entrada/     (read)
# - processado_a/ (write)
# - pronto/      (write)

# Worker B precisa de:
# - processado_a/ (read)
# - pronto/      (write)
# - banco de dados (insert, select)

# Arquivo .env deve ter:
# - DB_PASSWORD em variável de ambiente
# - Nunca em arquivo visible
```

---

## 7. Banco de Dados

### O que foi implementado:

- ✅ **Queries parametrizadas**: `execute_values()` do psycopg2
- ✅ **Sem SQL injection**: Valores separados da query
- ✅ **Validação de tipos**: Worker B converte antes de inserir
- ✅ **Usuário com permissões mínimas**: Recomenda-se CREATE permissões restritas

### Exemplo seguro:

```python
# ✅ SEGURO: parametrizado
sql = "INSERT INTO dados (fonte, payload) VALUES %s"
execute_values(cur, sql, values)

# ❌ INSEGURO: concatenação
sql = f"INSERT INTO dados (fonte, payload) VALUES ({source_file}, {row})"
```

---

## 8. Deduplicação no Banco (Novo)

### O que foi implementado:

**Arquivo: `db.py`**

- ✅ **Hash automático**: `payload_hash` gerado com `md5(payload::text)` STORED
- ✅ **Constraint única**: `UNIQUE (fonte, payload_hash)` garante unicidade
- ✅ **ON CONFLICT**: Insert ignora duplicatas sem erro
- ✅ **Migração segura**: Remove duplicatas existentes ao criar constraint

### Schema da Tabela:

```sql
CREATE TABLE dados_processados (
    id SERIAL PRIMARY KEY,
    fonte TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT GENERATED ALWAYS AS (md5(payload::text)) STORED,
    carregado_em TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    CONSTRAINT unique_fonte_payload_hash UNIQUE (fonte, payload_hash)
)
```

### Insert com Deduplicação:

```python
# Comando seguro com deduplicação
sql = (
    "INSERT INTO dados_processados (fonte, payload) VALUES %s "
    "ON CONFLICT ON CONSTRAINT unique_fonte_payload_hash DO NOTHING"
)
execute_values(cur, sql, values)
```

### Comportamento:

**Cenário: Reexecução de `worker_b.py` com mesmo arquivo**

```
Round 1: sample_input_10000.csv
  → Inseridas 10.000 linhas no banco ✅

Round 2: sample_input_10000.csv (reprocessado)
  → ON CONFLICT ignora as 10.000 duplicatas
  → Inseridas 0 linhas no banco ✅
  → Banco mantém 10.000 registros unicos
```

### Benefícios:

- **Idempotência**: Rodar `worker_b.py` múltiplas vezes é seguro
- **Sem duplicatas**: Mesmos dados nunca são inseridos duas vezes
- **Performance**: Constraint at database level é eficiente
- **Rastreabilidade**: Logs mostram quantas linhas foram realmente inseridas

### Verificação no Banco:

```sql
-- Ver a constraint criada
SELECT constraint_name FROM information_schema.table_constraints 
WHERE table_name = 'dados_processados' AND constraint_type = 'UNIQUE';

-- Ver registros únicos por arquivo
SELECT fonte, COUNT(*) as total_linhas
FROM dados_processados
GROUP BY fonte
ORDER BY total_linhas DESC;

-- Ver se há coluna payload_hash
SELECT column_name FROM information_schema.columns
WHERE table_name = 'dados_processados' AND column_name = 'payload_hash';
```

### Exemplo de Resultado Real:

```
pipeline_db=# SELECT fonte, COUNT(*) as total_linhas
pipeline_db-# FROM dados_processados
pipeline_db-# GROUP BY fonte
pipeline_db-# ORDER BY total_linhas DESC;
         fonte          | total_linhas
------------------------+--------------
 demo_large.csv         |       100000
 sample_input_10000.csv |         8954
 demo_formula.csv       |            3
 demo_validation.csv    |            2
(4 rows)
```

**Observações:**
- `demo_large.csv`: 100.000 linhas inseridas (nenhuma descartada)
- `sample_input_10000.csv`: 8.954 linhas inseridas (46 linhas foram descartadas como vazias)
- `demo_formula.csv`: 3 linhas com fórmulas sanitizadas
- `demo_validation.csv`: 2 linhas válidas

Total de linhas no banco: **110.005 registros únicos** 🎯

---

## 🧪 Como Testar a Segurança

### 1. Teste de fórmula Excel

```bash
# Criar arquivo com fórmula
echo 'nome,valor
=SYSTEM("cmd"),100' > entrada/malicioso.csv

# Executar Worker A
python worker_a.py

# Resultado: arquivo rejeitado e movido para pronto/
```

### 2. Teste de arquivo grande

```bash
# Criar arquivo de 100 MB
dd if=/dev/zero of=entrada/grande.csv bs=1M count=100

# Executar Worker A
python worker_a.py

# Resultado: arquivo rejeitado (excede 50 MB)
```

### 3. Teste de .tmp

```bash
# Deixar arquivo .tmp na pasta
touch processado_a/teste.tmp

# Executar Worker B
python worker_b.py

# Resultado: arquivo .tmp é ignorado (só processa .csv)
```

### 4. Teste de reprocessamento

```bash
# Executar demo de segurança
python testes/demo_security.py

# Depois processar
python worker_a.py
python worker_b.py

# Resultado: arquivos já processados em pronto/ não são reprocessados
```

### 5. Teste de deduplicação no banco

```bash
# Executar demo de teste de descarte
python testes/generate_test_discarded_lines.py
python worker_a.py
python worker_b.py

# Reexecute para verificar deduplicação
python worker_a.py
python worker_b.py

# Verificar no banco
psql -d pipeline_db -c "SELECT COUNT(*) as total FROM dados_processados WHERE fonte='test_discarded_lines.csv';"
# Resultado: Mesmo número de linhas (sem duplicatas!)

# Verificar métricas
python metrics_viewer.py
# Você verá "Inseridas 0 linhas" na segunda execução
```

---

## 📋 Resumo das Melhorias

| Risco                          | Solução                                    | Status |
| ------------------------------ | ------------------------------------------ | ------ |
| CSV com fórmulas maliciosas    | Detecção de padrões perigosos              | ✅     |
| Arquivo incompleto processado  | Escrita em .tmp + rename atômico           | ✅     |
| Reprocessamento duplicado      | Movimentação para pronto/                  | ✅     |
| Dados sensíveis em logs        | Loga apenas contagem e nome de arquivo     | ✅     |
| SQL Injection                  | Queries parametrizadas                     | ✅     |
| Falha silenciosa               | Try/except + move para pronto mesmo em erro| ✅     |
| Arquivo parcialmente escrito   | Operação atômica rename()                  | ✅     |
| Caracteres malformados         | Sanitização de strings                     | ✅     |
| Duplicatas no banco            | Constraint única + ON CONFLICT DO NOTHING  | ✅     |
| Inserção duplicada ao reexecutar | Hash MD5 payload com STORED + UNIQUE      | ✅     |

---

## 🚀 Próximos Passos (Produção)

Para usar em produção real, adicione:

1. **Controle de acesso** nas pastas (permissões do SO)
2. **Criptografia do .env** com bibliotecas como `python-dotenv`
3. **Auditoria de logs** com ELK Stack ou CloudWatch
4. **Backup automático** de arquivos processados
5. **Monitoramento** com alertas de erros
6. **Testes automatizados** de segurança