"""
exportar_amostras.py
-----------------------------------------------------------------------------
Exporta os resultados do pipeline (Silver/Gold/log) para CSV em
`exemplo_saida/`, para que qualquer pessoa consiga conferir os números
SEM precisar instalar Spark (abre no Excel).

Uso (depois de rodar `python src/orchestrator.py`):

    python scripts/exportar_amostras.py
"""
import csv
import shutil
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))

from pyspark.sql import functions as F  # noqa: E402

import config  # noqa: E402
from spark_session import get_spark  # noqa: E402

SAIDA = RAIZ / "exemplo_saida"


def _gravar_csv(df, destino: Path, ordenar=None):
    if ordenar:
        df = df.orderBy(*ordenar)
    linhas = df.collect()
    with destino.open("w", newline="", encoding="utf-8-sig") as f:   # BOM: Excel abre com acentos
        w = csv.writer(f)
        w.writerow(df.columns)
        for r in linhas:
            w.writerow(["" if v is None else v for v in r])
    print(f"  {destino.name}: {len(linhas)} linhas")


def main():
    SAIDA.mkdir(exist_ok=True)
    spark = get_spark("exportar-amostras")
    try:
        print("Exportando para", SAIDA)
        _gravar_csv(spark.read.parquet(str(config.SILVER_VENDAS)), SAIDA / "silver_vendas_unificadas.csv",
                    ["fonte", "id_transacao", "sku"])
        _gravar_csv(spark.read.parquet(str(config.SILVER_QUARENTENA)), SAIDA / "silver_quarentena.csv",
                    ["motivo_rejeicao", "fonte", "id_transacao"])
        _gravar_csv(spark.read.parquet(str(config.GOLD_VENDAS_MENSAL_CATEGORIA)),
                    SAIDA / "gold_vendas_mensal_categoria.csv", ["ano_mes", "categoria", "fonte"])
        _gravar_csv(spark.read.parquet(str(config.GOLD_TOP_PRODUTOS)), SAIDA / "gold_top_produtos.csv",
                    [F.col("receita_total").desc()])
    finally:
        spark.stop()
    for nome in ("pipeline_execution_log.csv", "arquivos_processados.csv"):
        origem = config.CONTROL_DIR / nome
        if origem.exists():
            shutil.copy(origem, SAIDA / nome)
            print(f"  {nome}: copiado")


if __name__ == "__main__":
    main()
