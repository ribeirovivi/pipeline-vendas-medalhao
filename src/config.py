"""
config.py
-----------------------------------------------------------------------------
Caminhos centrais do projeto. Mantidos em um único lugar para que as outras
camadas (bronze, silver, gold) e o orquestrador não precisem repetir strings
de caminho espalhadas pelo código.
"""

import os
from pathlib import Path

# PIPELINE_BASE_DIR permite apontar o pipeline para outra pasta (usado pelos
# testes automatizados, para nao tocar nos dados reais).
BASE_DIR = Path(os.environ.get("PIPELINE_BASE_DIR") or Path(__file__).resolve().parent.parent)

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
REF_DIR = DATA_DIR / "referencia"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

CONTROL_DIR = BASE_DIR / "control"
ARQUIVOS_PROCESSADOS_PATH = CONTROL_DIR / "arquivos_processados.csv"
LOG_EXECUCOES_PATH = CONTROL_DIR / "pipeline_execution_log.csv"

DIM_PRODUTO_PATH = REF_DIR / "dim_produto.csv"

# Fontes brutas — cada uma com seu próprio formato (o problema que o
# pipeline resolve)
FONTE_LOJA_ONLINE = RAW_DIR / "loja_online"
FONTE_LOJA_FISICA = RAW_DIR / "loja_fisica"
FONTE_MARKETPLACE = RAW_DIR / "marketplace"

# Tabelas Bronze (uma por fonte, schema bruto + metadados de ingestão)
BRONZE_LOJA_ONLINE = BRONZE_DIR / "loja_online"
BRONZE_LOJA_FISICA = BRONZE_DIR / "loja_fisica"
BRONZE_MARKETPLACE = BRONZE_DIR / "marketplace"

# Tabelas Silver
SILVER_VENDAS = SILVER_DIR / "vendas_unificadas"
SILVER_QUARENTENA = SILVER_DIR / "vendas_quarentena"

# Tabelas Gold
GOLD_VENDAS_MENSAL_CATEGORIA = GOLD_DIR / "vendas_mensal_categoria"
GOLD_TOP_PRODUTOS = GOLD_DIR / "top_produtos"
