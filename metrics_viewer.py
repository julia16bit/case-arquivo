"""
Script para visualizar métricas do pipeline.
Exibe sumário geral e detalhes por arquivo.
"""

import json
from pathlib import Path
from colored_logging import setup_colored_logging
from metrics import get_tracker

logger = setup_colored_logging("metrics_viewer")


def print_header(title):
    """Imprime um cabeçalho formatado."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_file_metrics(tracker):
    """Exibe métricas detalhadas por arquivo."""
    print_header("📊 MÉTRICAS POR ARQUIVO")
    
    metrics = tracker.metrics.get("files", {})
    if not metrics:
        print("  Nenhum arquivo processado ainda.")
        return
    
    for filename, data in sorted(metrics.items()):
        print(f"\n  📄 {filename}")
        
        worker_a = data.get("worker_a", {})
        if worker_a:
            print(f"    Worker A:")
            print(f"      ⏱️  Tempo: {worker_a.get('duration_seconds', 0):.2f}s")
            print(f"      ✅ Linhas processadas: {worker_a.get('lines_processed', 0)}")
            print(f"      ❌ Linhas descartadas: {worker_a.get('lines_discarded', 0)}")
            if worker_a.get("error"):
                print(f"      ⚠️  Erro: {worker_a['error']}")
        
        worker_b = data.get("worker_b", {})
        if worker_b:
            print(f"    Worker B:")
            print(f"      ⏱️  Tempo: {worker_b.get('duration_seconds', 0):.2f}s")
            print(f"      ✅ Linhas processadas: {worker_b.get('lines_processed', 0)}")
            print(f"      ❌ Linhas descartadas: {worker_b.get('lines_discarded', 0)}")
            if worker_b.get("error"):
                print(f"      ⚠️  Erro: {worker_b['error']}")
        
        if data.get("total_time"):
            print(f"    Total: {data['total_time']:.2f}s")
        
        if data.get("errors"):
            print(f"    Erros registrados: {len(data['errors'])}")


def print_summary_metrics(tracker):
    """Exibe sumário agregado."""
    print_header("📈 RESUMO DO PIPELINE")
    
    summary = tracker.get_summary()
    
    print(f"\n  Arquivos processados: {summary['total_files_processed']}")
    print(f"  Total de erros: {summary['total_errors']}")
    print(f"  Linhas médias por arquivo: {int(summary['avg_lines_per_file'])}")
    print(f"  Tempo médio por arquivo: {summary['avg_time_per_file']:.2f}s")
    print(f"  Taxa de processamento: {summary['files_per_minute']:.2f} arquivos/minuto")


def main():
    """Função principal."""
    tracker = get_tracker()
    
    print_header("🔍 VISUALIZADOR DE MÉTRICAS DO PIPELINE")
    
    if not tracker.metrics.get("files"):
        print("\n  ⚠️  Nenhuma métrica disponível. Execute os workers primeiro!")
        print("\n  Comandos:")
        print("    python worker_a.py")
        print("    python worker_b.py")
        return
    
    print_summary_metrics(tracker)
    print_file_metrics(tracker)
    
    print("\n" + "=" * 70)
    print(f"  Arquivo de métricas: metrics.json")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
