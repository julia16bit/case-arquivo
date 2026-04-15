from pathlib import Path
import csv

# Gera um arquivo CSV de exemplo com 10.000 linhas, contendo variações de dados para testar os workers
path = Path('entrada/sample_input_10000.csv')
path.parent.mkdir(parents=True, exist_ok=True)
with path.open('w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['nome', 'idade', 'salario'])
    for i in range(1, 10001):
        if i % 5 == 0:
            writer.writerow([f'Cliente{i}', '', f'{3000 + (i % 50) * 10}'])
        elif i % 7 == 0:
            writer.writerow(['', f'{20 + (i % 40)}', f'{3500 + (i % 60) * 15}'])
        elif i % 11 == 0:
            writer.writerow([f'Cliente{i}', f'{25 + (i % 30)}', ''])
        else:
            writer.writerow([f'Cliente{i}', f'{25 + (i % 40)}', f'{3000 + (i % 100) * 20}'])
print(f'Arquivo criado: {path} ({sum(1 for _ in path.open(encoding="utf-8")) - 1} linhas)')
