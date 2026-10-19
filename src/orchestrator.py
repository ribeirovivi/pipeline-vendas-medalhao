"""
orchestrator.py
-----------------------------------------------------------------------------
ORQUESTRAÇÃO — executa Bronze -> Silver -> Gold em sequência, com logging
e tratamento de erro por etapa.

Isto é, propositalmente, uma orquestração simples (um script Python com uma
sequência de chamadas) — não usei Airflow nem Databricks Jobs porque o
objetivo aqui é ter um projeto 100% autocontido, fácil de rodar em qualquer
máquina para fins de estudo/portfólio.

Se fosse evoluir isso para produção, o próximo passo natural seria:
- Databricks Jobs / Workflows (se estiver no Databricks), com cada camada
  como uma task, e dependências entre elas (bronze -> silver -> gold);
- ou Apache Airflow, com uma DAG equivalente e retries automáticos.

A lógica de negócio (o que cada camada faz) não muda — só a forma de
agendar e monitorar a execução muda.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ingest_bronze
import transform_silver
import aggregate_gold
from spark_session import get_spark


def main():
    spark = get_spark()
    try:
        print("\n=== ETAPA 1/3 — BRONZE (ingestão incremental) ===")
        ingest_bronze.executar(spark)

        print("\n=== ETAPA 2/3 — SILVER (padronização, integração, qualidade) ===")
        transform_silver.executar(spark)

        print("\n=== ETAPA 3/3 — GOLD (agregações de negócio) ===")
        aggregate_gold.executar(spark)

        print("\nPipeline concluído com sucesso.")
    except Exception as e:
        print(f"\n[ERRO] Pipeline interrompido: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
