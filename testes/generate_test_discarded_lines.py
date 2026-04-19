"""
Script para gerar um CSV de teste com linhas que serão descartadas.
Linhas são descartadas quando TODAS as colunas ficam vazias após sanitização.
"""

import csv
from pathlib import Path

def generate_test_file():
    """Gera arquivo CSV com linhas que serão descartadas durante processamento."""
    entrada_dir = Path("entrada")
    entrada_dir.mkdir(exist_ok=True)
    
    file_path = entrada_dir / "test_discarded_lines.csv"
    
    # Dados com linhas que serão descartadas
    rows = [
        # Linha 1: Válida
        {"nome": "João Silva", "email": "joao@example.com", "idade": "30"},
        
        # Linha 2: Válida
        {"nome": "Maria Santos", "email": "maria@example.com", "idade": "25"},
        
        # Linha 3: Será descartada (todas as colunas contêm apenas fórmulas perigosas)
        {"nome": "=SYSTEM('cmd')", "email": "@sumproduct(1+9)*cmd", "idade": "+2+5+cmd"},
        
        # Linha 4: Válida (tem dados após sanitização)
        {"nome": "Pedro Costa", "email": "pedro@example.com", "idade": "35"},
        
        # Linha 5: Será descartada (todas as colunas são vazias)
        {"nome": "", "email": "", "idade": ""},
        
        # Linha 6: Será descartada (todas as colunas com só espaços)
        {"nome": "   ", "email": "   ", "idade": "   "},
        
        # Linha 7: Válida (tem dados)
        {"nome": "Ana Costa", "email": "ana@example.com", "idade": "28"},
        
        # Linha 8: Será descartada (todas as colunas com caracteres perigosos)
        {"nome": "[malicioso]", "email": "{perigoso}", "idade": "<script>"},
        
        # Linha 9: Parcialmente válida (nome válido, outros vazios)
        {"nome": "Carlos Silva", "email": "", "idade": ""},
        
        # Linha 10: Será descartada (só contém caracteres especiais perigosos)
        {"nome": "^^^~~~", "email": "<<<>>>", "idade": "|||"},
    ]
    
    # Escrever CSV
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["nome", "email", "idade"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"✅ Arquivo criado: {file_path}")
    print(f"   Total de linhas: {len(rows)}")
    print("\n📊 Comportamento esperado:")
    print("   Linhas VÁLIDAS (processadas): 4")
    print("     - Linha 1: João Silva, joao@example.com, 30")
    print("     - Linha 2: Maria Santos, maria@example.com, 25")
    print("     - Linha 4: Pedro Costa, pedro@example.com, 35")
    print("     - Linha 7: Ana Costa, ana@example.com, 28")
    print("     - Linha 9: Carlos Silva (outros campos vazios)")
    print("\n   Linhas DESCARTADAS (removidas): 5")
    print("     - Linha 3: Todas as colunas contêm fórmulas Excel perigosas")
    print("     - Linha 5: Todas as colunas vazias")
    print("     - Linha 6: Todas as colunas com só espaços")
    print("     - Linha 8: Todas as colunas contêm caracteres perigosos")
    print("     - Linha 10: Todas as colunas contêm caracteres especiais perigosos")
    print("\n💡 Próximos passos:")
    print("   1. python worker_a.py")
    print("   2. python worker_b.py")
    print("   3. python metrics_viewer.py")
    print("\n   Você verá:")
    print("   - Worker A: Linhas processadas: 5 | Linhas descartadas: 5")
    print("   - Worker B: Linhas processadas: 5 | Linhas descartadas: 0")

if __name__ == "__main__":
    generate_test_file()
