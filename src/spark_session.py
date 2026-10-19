"""
spark_session.py
-----------------------------------------------------------------------------
Cria a SparkSession usada por todas as camadas do pipeline. Centralizado
aqui para que, se um dia você mover isso para o Databricks (onde a variável
`spark` já vem pronta no notebook), baste remover essa criação manual — o
resto do código (as transformações) não muda.
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "pipeline-vendas-medalhao") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # roda localmente usando todos os núcleos disponíveis
        .config("spark.sql.session.timeZone", "UTC")
        # Volume pequeno: 200 particoes (padrao) so geram sobrecarga. Com 4, o
        # pipeline roda varias vezes mais rapido localmente.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
