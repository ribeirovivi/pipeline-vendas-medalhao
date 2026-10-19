"""
aggregate_gold.py
-----------------------------------------------------------------------------
CAMADA GOLD — agregações de negócio.

Aqui os dados já estão limpos, íntegros e unificados (Silver) — a camada
Gold existe para responder perguntas de negócio diretamente, sem que quem
for consumir (analista, BI, dashboard) precise reprocessar nada.

Duas tabelas Gold neste projeto:

1. `vendas_mensal_categoria` — receita e quantidade vendida por mês,
   categoria e canal. Granularidade pensada para um dashboard executivo
   (ex: Power BI) mostrar evolução mensal por categoria/canal.

2. `top_produtos` — ranking de produtos por receita total, útil para
   perguntas do tipo "quais produtos mais vendem, independente do canal".
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import config
from utils_logging import CronometroEtapa, registrar_execucao


def executar(spark: SparkSession):
    with CronometroEtapa() as cron:
        vendas = spark.read.parquet(str(config.SILVER_VENDAS))

        vendas_mensal_categoria = (
            vendas
            .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
            .groupBy("ano_mes", "categoria", "fonte")
            .agg(
                F.sum("valor_total").alias("receita_total"),
                F.sum("quantidade").alias("quantidade_total"),
                F.countDistinct("id_transacao").alias("qtd_transacoes")
            )
            .orderBy("ano_mes", "categoria", "fonte")
        )

        top_produtos = (
            vendas
            .groupBy("sku", "nome_produto", "categoria")
            .agg(
                F.sum("valor_total").alias("receita_total"),
                F.sum("quantidade").alias("quantidade_total")
            )
            .orderBy(F.col("receita_total").desc())
        )

        config.GOLD_DIR.mkdir(parents=True, exist_ok=True)
        vendas_mensal_categoria.write.mode("overwrite").parquet(str(config.GOLD_VENDAS_MENSAL_CATEGORIA))
        top_produtos.write.mode("overwrite").parquet(str(config.GOLD_TOP_PRODUTOS))

        n_linhas = vendas_mensal_categoria.count() + top_produtos.count()

    registrar_execucao("gold", "vendas_mensal_categoria+top_produtos",
                        n_linhas, n_linhas, 0, cron.duracao_segundos, "sucesso")

    print("[gold] vendas_mensal_categoria:")
    vendas_mensal_categoria.show(10, truncate=False)
    print("[gold] top_produtos:")
    top_produtos.show(10, truncate=False)


if __name__ == "__main__":
    from spark_session import get_spark
    spark = get_spark()
    executar(spark)
    spark.stop()
