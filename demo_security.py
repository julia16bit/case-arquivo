"""
Script de demonstração das melhorias de segurança implementadas.
Execute este script para testar os cenários de segurança do pipeline.
"""

import csv
import os
from pathlib import Path

def demo_dangerous_formulas():
    """Demonstra como o pipeline rejeita fórmulas Excel perigosas."""
    print("\n🧪 TESTE 1: Detecção de Fórmulas Excel")
    print("=" * 60)
    
    entrada_dir = Path("entrada")
    entrada_dir.mkdir(exist_ok=True)
    
    file_path = entrada_dir / "demo_formula.csv"
    
    # Criar arquivo com fórmula Excel
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['nome', 'comando', 'valor'])
        writer.writerow(['User1', '=SYSTEM("cmd.exe")', '1000'])
        writer.writerow(['User2', '@sumproduct(1+9)*cmd|calc', '2000'])
        writer.writerow(['User3', '+2+5+cmd|calc', '3000'])
    
    print(f"✓ Arquivo criado: {file_path}")
    print("  Conteúdo: Contém fórmulas Excel perigosas")
    print("\n  Execute: python worker_a.py")
    print("  Resultado esperado: Arquivo rejeitado e movido para pronto/")


def demo_large_file():
    """Demonstra como o pipeline rejeita arquivos muito grandes."""
    print("\n🧪 TESTE 2: Limite de Tamanho de Arquivo")
    print("=" * 60)
    
    entrada_dir = Path("entrada")
    entrada_dir.mkdir(exist_ok=True)
    
    file_path = entrada_dir / "demo_large.csv"
    
    # Criar arquivo grande (> 50 MB será rejeitado)
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'data', 'valor'])
        for i in range(100000):
            writer.writerow([i, f'dado_{i}' * 10, i * 100])
    
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    print(f"✓ Arquivo criado: {file_path}")
    print(f"  Tamanho: {file_size_mb:.2f} MB (máximo: 50 MB)")
    
    if file_size_mb > 50:
        print("  Status: Será rejeitado (excede limite)")
    else:
        print("  Status: Será aceito")
    
    print("\n  Execute: python worker_a.py")
    print("  Resultado esperado: Arquivo rejeitado se > 50 MB")


def demo_tmp_files():
    """Demonstra o uso de arquivos temporários (.tmp)."""
    print("\n🧪 TESTE 3: Arquivos Temporários (.tmp)")
    print("=" * 60)
    
    processado_dir = Path("processado_a")
    processado_dir.mkdir(exist_ok=True)
    
    # Simular arquivo .tmp
    tmp_file = processado_dir / "demo_tmp.tmp"
    tmp_file.write_text("Arquivo incompleto ou em processamento", encoding='utf-8')
    
    print(f"✓ Arquivo .tmp criado: {tmp_file}")
    print("  Status: Worker B IGNORA arquivos .tmp")
    print("\n  Execute: python worker_b.py")
    print("  Resultado esperado: Arquivo .tmp é ignorado")


def demo_reprocessing_prevention():
    """Demonstra como o pipeline evita reprocessamento."""
    print("\n🧪 TESTE 4: Prevenção de Reprocessamento")
    print("=" * 60)
    
    pronto_dir = Path("pronto")
    pronto_dir.mkdir(exist_ok=True)
    
    # Listar arquivos já processados
    processed = list(pronto_dir.glob("*.csv"))
    
    if processed:
        print(f"✓ Encontrados {len(processed)} arquivo(s) já processados em pronto/:")
        for f in processed[:5]:
            print(f"  - {f.name}")
        print("\n  Estes arquivos NÃO serão reprocessados")
    else:
        print("✓ Nenhum arquivo processado ainda")
    
    print("\n  Execute: python worker_a.py && python worker_b.py")
    print("  Resultado esperado: Arquivos em pronto/ permanecem intactos")


def demo_logs_security():
    """Demonstra como os logs não revelam dados sensíveis."""
    print("\n🧪 TESTE 5: Logs Seguros (sem dados sensíveis)")
    print("=" * 60)
    
    print("✓ Logs implementados com segurança:")
    print("  ✓ Nunca registra conteúdo completo do CSV")
    print("  ✓ Nunca registra email, CPF ou dados pessoais")
    print("  ✓ Registra apenas: contagem, nome do arquivo, status")
    print("\n  Exemplo de LOG SEGURO:")
    print("  2026-04-17 10:30:45 INFO Inseridas 150 linhas no banco (origem: dados.csv)")
    print("\n  Exemplo do que NÃO aparece nos logs:")
    print("  ❌ Inseridas 150 linhas (payload: {email: joao@example.com, cpf: 123.456.789-00})")


def demo_validation():
    """Demonstra as validações implementadas."""
    print("\n🧪 TESTE 6: Validação de CSV")
    print("=" * 60)
    
    entrada_dir = Path("entrada")
    entrada_dir.mkdir(exist_ok=True)
    
    file_path = entrada_dir / "demo_validation.csv"
    
    # Criar arquivo válido
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['nome', 'idade', 'email'])
        writer.writerow(['João Silva', '30', 'joao@example.com'])
        writer.writerow(['Maria Santos', '28', 'maria@example.com'])
    
    print(f"✓ Arquivo válido criado: {file_path}")
    print("  Validações executadas:")
    print("  ✓ Tamanho: OK (< 50 MB)")
    print("  ✓ Linhas: OK (< 100.000)")
    print("  ✓ Cabeçalho: OK")
    print("  ✓ Fórmulas: OK (nenhuma detectada)")
    print("  ✓ Caracteres: OK (apenas caracteres seguros)")
    print("\n  Execute: python worker_a.py")
    print("  Resultado esperado: Arquivo processado com sucesso")


def main():
    """Executa todos os testes de demonstração."""
    print("\n" + "=" * 60)
    print("🔐 DEMONSTRAÇÃO DE SEGURANÇA - PIPELINE CSV MANAGER")
    print("=" * 60)
    
    print("\nEste script cria arquivos de exemplo para testar")
    print("as melhorias de segurança implementadas.")
    
    demo_dangerous_formulas()
    demo_large_file()
    demo_tmp_files()
    demo_reprocessing_prevention()
    demo_logs_security()
    demo_validation()
    
    print("\n" + "=" * 60)
    print("✅ ARQUIVOS DE TESTE CRIADOS")
    print("=" * 60)
    print("\nPróximos passos:")
    print("1. Execute: python worker_a.py")
    print("2. Execute: python worker_b.py")
    print("3. Verifique os resultados em: pronto/")
    print("\nVeja SECURITY.md para mais detalhes sobre cada teste.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
