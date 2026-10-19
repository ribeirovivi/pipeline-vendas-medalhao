"""
utils_logging.py
-----------------------------------------------------------------------------
Duas responsabilidades, propositalmente simples (sem depender de um
orquestrador externo tipo Airflow, para o projeto ficar 100% autocontido):

1. `ArquivosProcessados` — controla quais arquivos brutos já foram
   ingeridos na camada Bronze, usando um hash MD5 do conteúdo. É essa tabela
   de controle que permite a CARGA INCREMENTAL: a cada execução, o pipeline
   só processa arquivos novos ou que mudaram desde a última vez — arquivos
   já processados e inalterados são pulados.

2. `registrar_execucao` — grava uma linha de log a cada execução de uma
   camada do pipeline (linhas lidas, linhas gravadas, linhas rejeitadas,
   duração, status). É a base do MONITORAMENTO do pipeline: dá para abrir
   `control/pipeline_execution_log.csv` a qualquer momento e responder
   "quando essa tabela foi atualizada pela última vez? quantas linhas
   entraram? algo falhou?".
"""

import csv
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone

from config import ARQUIVOS_PROCESSADOS_PATH, LOG_EXECUCOES_PATH, CONTROL_DIR


def _hash_arquivo(caminho: Path) -> str:
    h = hashlib.md5()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(8192), b""):
            h.update(bloco)
    return h.hexdigest()


class ArquivosProcessados:
    """Controle de idempotência/incremental baseado em hash de arquivo."""

    CAMPOS = ["caminho_arquivo", "hash_md5", "processado_em", "status"]

    def __init__(self, caminho_controle: Path = ARQUIVOS_PROCESSADOS_PATH):
        self.caminho_controle = caminho_controle
        self.caminho_controle.parent.mkdir(parents=True, exist_ok=True)
        self._registros = self._carregar()

    def _carregar(self):
        if not self.caminho_controle.exists():
            return {}
        with open(self.caminho_controle, newline="", encoding="utf-8") as f:
            return {row["caminho_arquivo"]: row for row in csv.DictReader(f)}

    def precisa_processar(self, caminho_arquivo: Path) -> bool:
        chave = str(caminho_arquivo)
        hash_atual = _hash_arquivo(caminho_arquivo)
        registro = self._registros.get(chave)
        if registro is None:
            return True
        if registro["status"] != "sucesso":
            return True
        return registro["hash_md5"] != hash_atual

    def marcar_processado(self, caminho_arquivo: Path, status: str = "sucesso"):
        chave = str(caminho_arquivo)
        self._registros[chave] = {
            "caminho_arquivo": chave,
            "hash_md5": _hash_arquivo(caminho_arquivo),
            "processado_em": datetime.now(timezone.utc).isoformat(),
            "status": status
        }
        self._salvar()

    def _salvar(self):
        with open(self.caminho_controle, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.CAMPOS)
            writer.writeheader()
            writer.writerows(self._registros.values())


class CronometroEtapa:
    """Context manager simples para medir a duração de uma etapa do pipeline."""

    def __enter__(self):
        self.inicio = time.perf_counter()
        self.inicio_ts = datetime.now(timezone.utc).isoformat()
        return self

    def __exit__(self, *exc):
        self.duracao_segundos = round(time.perf_counter() - self.inicio, 3)
        self.fim_ts = datetime.now(timezone.utc).isoformat()


def registrar_execucao(camada, fonte, linhas_lidas, linhas_gravadas,
                        linhas_rejeitadas, duracao_segundos, status,
                        caminho_log: Path = LOG_EXECUCOES_PATH,
                        linhas_duplicadas: int = 0):
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    existe = caminho_log.exists()
    with open(caminho_log, "a", newline="", encoding="utf-8") as f:
        campos = ["timestamp", "camada", "fonte", "linhas_lidas", "linhas_gravadas",
                  "linhas_rejeitadas", "linhas_duplicadas", "duracao_segundos", "status"]
        writer = csv.DictWriter(f, fieldnames=campos)
        if not existe:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "camada": camada,
            "fonte": fonte,
            "linhas_lidas": linhas_lidas,
            "linhas_gravadas": linhas_gravadas,
            "linhas_rejeitadas": linhas_rejeitadas,
            "linhas_duplicadas": linhas_duplicadas,
            "duracao_segundos": duracao_segundos,
            "status": status
        })
