import json
import time
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from colored_logging import setup_colored_logging

logger = setup_colored_logging("metrics")

METRICS_FILE = Path("metrics.json")


class MetricsTracker:
    """Rastreia métricas de processamento do pipeline."""
    
    def __init__(self):
        self.current_file = None
        self.start_time = None
        self.worker_a_start = None
        self.worker_b_start = None
        self.metrics = self._load_metrics()
    
    def _load_metrics(self):
        """Carrega métricas existentes do arquivo JSON."""
        if METRICS_FILE.exists():
            try:
                with METRICS_FILE.open('r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as exc:
                logger.warning("Erro ao carregar métricas: %s", exc)
                return {"files": {}, "summary": {}}
        return {"files": {}, "summary": {"total_files": 0, "total_errors": 0, "start_time": datetime.now().isoformat()}}
    
    def _save_metrics(self):
        """Salva métricas em arquivo JSON."""
        try:
            with METRICS_FILE.open('w', encoding='utf-8') as f:
                json.dump(self.metrics, f, indent=2, ensure_ascii=False)
        except Exception as exc:
            logger.error("Erro ao salvar métricas: %s", exc)
    
    def start_file_processing(self, filename, worker):
        """Marca o início do processamento de um arquivo em um worker."""
        self.current_file = filename
        self.start_time = time.time()
        
        if filename not in self.metrics["files"]:
            self.metrics["files"][filename] = {
                "worker_a": {},
                "worker_b": {},
                "total_time": 0,
                "errors": []
            }
        
        if worker == "a":
            self.worker_a_start = time.time()
        elif worker == "b":
            self.worker_b_start = time.time()
    
    def end_file_processing(self, worker, lines_processed, lines_discarded=0, error=None):
        """Marca o fim do processamento de um arquivo em um worker."""
        if self.current_file is None:
            return
        
        elapsed = time.time() - self.start_time
        
        if worker == "a":
            self.metrics["files"][self.current_file]["worker_a"] = {
                "duration_seconds": round(elapsed, 2),
                "lines_processed": lines_processed,
                "lines_discarded": lines_discarded,
                "error": error
            }
        elif worker == "b":
            self.metrics["files"][self.current_file]["worker_b"] = {
                "duration_seconds": round(elapsed, 2),
                "lines_processed": lines_processed,
                "lines_discarded": lines_discarded,
                "error": error
            }
        
        if error:
            self.metrics["files"][self.current_file]["errors"].append({
                "worker": worker,
                "timestamp": datetime.now().isoformat(),
                "message": str(error)
            })
            self.metrics["summary"]["total_errors"] += 1
        
        self._save_metrics()
    
    def complete_file(self, filename):
        """Marca arquivo como completamente processado (A e B)."""
        if filename in self.metrics["files"]:
            worker_a_time = self.metrics["files"][filename].get("worker_a", {}).get("duration_seconds", 0)
            worker_b_time = self.metrics["files"][filename].get("worker_b", {}).get("duration_seconds", 0)
            total_time = worker_a_time + worker_b_time
            
            self.metrics["files"][filename]["total_time"] = round(total_time, 2)
            self.metrics["files"][filename]["completed_at"] = datetime.now().isoformat()
            
            self.metrics["summary"]["total_files"] = len([f for f in self.metrics["files"].values() if "completed_at" in f])
            self._save_metrics()
    
    def get_file_metrics(self, filename):
        """Retorna métricas de um arquivo específico."""
        return self.metrics["files"].get(filename, {})
    
    def get_summary(self):
        """Retorna sumário agregado de todas as execuções."""
        completed_files = [f for f in self.metrics["files"].values() if "completed_at" in f]
        
        if not completed_files:
            return {
                "total_files_processed": 0,
                "total_errors": self.metrics["summary"].get("total_errors", 0),
                "avg_lines_per_file": 0,
                "avg_time_per_file": 0,
                "files_per_minute": 0
            }
        
        total_lines = sum(
            f.get("worker_a", {}).get("lines_processed", 0) + 
            f.get("worker_b", {}).get("lines_discarded", 0)
            for f in completed_files
        )
        avg_time = sum(f.get("total_time", 0) for f in completed_files) / len(completed_files)
        
        # Calcular taxa de processamento (arquivos por minuto)
        if self.metrics["summary"].get("start_time"):
            start = datetime.fromisoformat(self.metrics["summary"]["start_time"])
            elapsed_minutes = (datetime.now() - start).total_seconds() / 60
            files_per_minute = len(completed_files) / elapsed_minutes if elapsed_minutes > 0 else 0
        else:
            files_per_minute = 0
        
        return {
            "total_files_processed": len(completed_files),
            "total_errors": self.metrics["summary"].get("total_errors", 0),
            "avg_lines_per_file": round(total_lines / len(completed_files), 0) if completed_files else 0,
            "avg_time_per_file": round(avg_time, 2),
            "files_per_minute": round(files_per_minute, 2)
        }
    
    def print_metrics(self, filename=None):
        """Imprime métricas de forma legível."""
        if filename:
            metrics = self.get_file_metrics(filename)
            if metrics:
                logger.info("📊 Métricas para %s:", filename)
                logger.info("  Worker A: %0.2fs | %s linhas processadas | %s descartadas",
                    metrics.get("worker_a", {}).get("duration_seconds", 0),
                    metrics.get("worker_a", {}).get("lines_processed", 0),
                    metrics.get("worker_a", {}).get("lines_discarded", 0)
                )
                logger.info("  Worker B: %0.2fs | %s linhas processadas | %s descartadas",
                    metrics.get("worker_b", {}).get("duration_seconds", 0),
                    metrics.get("worker_b", {}).get("lines_processed", 0),
                    metrics.get("worker_b", {}).get("lines_discarded", 0)
                )
                logger.info("  Tempo total: %0.2fs", metrics.get("total_time", 0))
        else:
            summary = self.get_summary()
            logger.info("📊 Resumo do Pipeline:")
            logger.info("  Arquivos processados: %s", summary["total_files_processed"])
            logger.info("  Erros: %s", summary["total_errors"])
            logger.info("  Linhas médias por arquivo: %s", int(summary["avg_lines_per_file"]))
            logger.info("  Tempo médio por arquivo: %0.2fs", summary["avg_time_per_file"])
            logger.info("  Taxa: %0.2f arquivos/minuto", summary["files_per_minute"])


# Instância global
_tracker = MetricsTracker()


def get_tracker():
    """Retorna instância global do rastreador de métricas."""
    return _tracker
