"""
ingest_bronze.py
-----------------------------------------------------------------------------
CAMADA BRONZE — ingestão bruta.

Princípio da camada Bronze na arquitetura medalhão: os dados entram
"como vieram da origem", sem limpeza nem padronização. A única coisa que
adicionamos é METADADO DE INGESTÃO (de onde veio, qual arquivo, quando foi
ingerido) — isso é o que permite auditar depois "esse registro veio de
qual arquivo, processado quando?".

Cada fonte tem seu próprio schema bruto (é esperado que sejam diferentes —
a padronização acontece só na camada Silver).

Carga incremental: antes de ler um arquivo, verificamos em
`ArquivosProcessados` se ele já foi ingerido com sucesso e não mudou desde
então. Se sim, pulamos — só processamos o que é novo. Isso é o que
possibilita rodar este script todo dia sem reprocessar o histórico inteiro.
"""

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import config
from utils_logging import ArquivosProcessados, CronometroEtapa, registrar_execucao


def _adicionar_metadados(df: DataFrame, fonte: str, caminho_arquivo: Path) -> DataFrame:
    return (
        df
        .withColumn("_fonte_sistema", F.lit(fonte))
        .withColumn("_arquivo_origem", F.lit(caminho_arquivo.name))
        .withColumn("_ingerido_em", F.lit(datetime.now(timezone.utc).isoformat()))
    )


def _ingerir_arquivos(spark: SparkSession, pasta_origem: Path, destino_bronze: Path,
                       fonte: str, leitor, controle: ArquivosProcessados):
    """
    Percorre todos os arquivos de uma pasta de origem, ingere só os que são
    novos/alterados (checagem via `controle`) e acrescenta (append) na
    tabela Bronze correspondente em formato Parquet.
    """
    arquivos = sorted(pasta_origem.glob("*"))
    arquivos_a_processar = [a for a in arquivos if controle.precisa_processar(a)]

    if not arquivos_a_processar:
        print(f"[bronze:{fonte}] nada novo para processar (todos os arquivos já ingeridos).")
        registrar_execucao("bronze", fonte, 0, 0, 0, 0.0, "sem_novidade")
        return

    with CronometroEtapa() as cron:
        total_linhas = 0
        for arquivo in arquivos_a_processar:
            df_bruto = leitor(spark, arquivo)
            df_bronze = _adicionar_metadados(df_bruto, fonte, arquivo)

            destino_bronze.mkdir(parents=True, exist_ok=True)
            df_bronze.write.mode("append").parquet(str(destino_bronze))

            n_linhas = df_bronze.count()          # uma única ação (antes eram duas)
            total_linhas += n_linhas
            controle.marcar_processado(arquivo, status="sucesso")
            print(f"[bronze:{fonte}] ingerido {arquivo.name} ({n_linhas} linhas)")

    registrar_execucao("bronze", fonte, total_linhas, total_linhas, 0,
                        cron.duracao_segundos, "sucesso")


# --- Um "leitor" por fonte, porque cada uma tem um formato diferente -------

def _ler_loja_online(spark: SparkSession, caminho: Path) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .csv(str(caminho))
    )


def _ler_loja_fisica(spark: SparkSession, caminho: Path) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .csv(str(caminho))
    )


def _ler_marketplace(spark: SparkSession, caminho: Path) -> DataFrame:
    # JSON Lines: um pedido por linha, com um array `items` aninhado.
    # Aqui ainda NÃO fazemos o "explode" dos itens — isso é uma decisão de
    # padronização, então pertence à camada Silver. Bronze guarda o pedido
    # exatamente como chegou (aninhado).
    return spark.read.json(str(caminho))


def executar(spark: SparkSession):
    controle = ArquivosProcessados()

    _ingerir_arquivos(spark, config.FONTE_LOJA_ONLINE, config.BRONZE_LOJA_ONLINE,
                       "loja_online", _ler_loja_online, controle)
    _ingerir_arquivos(spark, config.FONTE_LOJA_FISICA, config.BRONZE_LOJA_FISICA,
                       "loja_fisica", _ler_loja_fisica, controle)
    _ingerir_arquivos(spark, config.FONTE_MARKETPLACE, config.BRONZE_MARKETPLACE,
                       "marketplace", _ler_marketplace, controle)


if __name__ == "__main__":
    from spark_session import get_spark
    spark = get_spark()
    executar(spark)
    spark.stop()
