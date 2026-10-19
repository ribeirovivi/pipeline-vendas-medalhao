"""
transform_silver.py
-----------------------------------------------------------------------------
CAMADA SILVER — padronização, integração e qualidade.

É aqui que o problema real do projeto é resolvido: as 3 fontes, cada uma
com seu schema, formato de data, formato de número e identificador de
produto, são transformadas num ÚNICO schema unificado:

    id_transacao | data | sku | categoria | nome_produto |
    quantidade | preco_unitario | valor_total | fonte | contexto

Decisões de design (e o porquê de cada uma):

1. INTEGRAÇÃO VIA TABELA DE REFERÊNCIA (dim_produto): a loja física usa um
   "código legado" próprio para os produtos, diferente do SKU usado pela
   loja online e pelo marketplace. Sem uma tabela de referência unificando
   os dois códigos, seria impossível comparar vendas dos 3 canais pelo
   mesmo produto. Isso é um problema de integração de dados extremamente
   comum no mundo real (sistemas legados com seu próprio cadastro).

2. GATE DE QUALIDADE (quarentena): registros com preço nulo, quantidade
   inválida (<= 0) ou produto não encontrado na tabela de referência NÃO
   entram na tabela Silver principal — vão para uma tabela separada de
   quarentena, com o motivo da rejeição. Isso evita que dado ruim
   "contamine" as métricas de negócio na camada Gold, e ainda deixa
   rastreável o que foi rejeitado e por quê (em vez de simplesmente
   descartar silenciosamente).

3. DEDUPLICAÇÃO DETERMINÍSTICA: a loja online, por exemplo, pode reenviar o
   mesmo pedido (problema comum em integrações via arquivo). Para cada chave
   (fonte, id_transacao, sku) mantemos UMA linha, escolhida por uma window
   function: a mais recente por `_ingerido_em` (desempate por quantidade e
   preço). Assim o resultado é sempre o mesmo, em qualquer execução. O número
   de linhas descartadas é contado e entra na RECONCILIAÇÃO:

       linhas lidas = válidas + rejeitadas (quarentena) + duplicadas

   Se essa conta não fechar, o pipeline interrompe com erro.

4. DATA VÁLIDA: registro cuja data não pôde ser interpretada vai para a
   quarentena (motivo `data_invalida`); antes, passava com data nula e
   contaminava o Gold (mês nulo).
"""

from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import config
from utils_logging import CronometroEtapa, registrar_execucao


def _carregar_dim_produto(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .csv(str(config.DIM_PRODUTO_PATH))
    )


def _padronizar_loja_online(spark: SparkSession, dim: DataFrame) -> DataFrame:
    bronze = spark.read.parquet(str(config.BRONZE_LOJA_ONLINE))
    return (
        bronze
        .withColumn("data", F.to_date("data_pedido", "yyyy-MM-dd"))
        .withColumn("preco_unitario", F.col("valor_unitario").cast("double"))
        .withColumn("quantidade", F.col("quantidade").cast("int"))
        .withColumnRenamed("id_pedido", "id_transacao")
        .withColumnRenamed("sku_produto", "sku")
        .withColumnRenamed("canal", "contexto")
        .join(F.broadcast(dim), on="sku", how="left")
        .withColumn("fonte", F.lit("loja_online"))
        .select("id_transacao", "data", "sku", "categoria", "nome_produto",
                "quantidade", "preco_unitario", "fonte", "contexto", "_ingerido_em")
    )


def _padronizar_loja_fisica(spark: SparkSession, dim: DataFrame) -> DataFrame:
    bronze = spark.read.parquet(str(config.BRONZE_LOJA_FISICA))
    dim_por_legado = dim.withColumnRenamed("sku", "sku_ref")
    return (
        bronze
        .withColumn("data", F.to_date("DT_VENDA", "dd/MM/yyyy"))
        .withColumn("preco_unitario", F.regexp_replace("VL_UNIT", ",", ".").cast("double"))
        .withColumn("quantidade", F.col("QTD").cast("int"))
        .withColumnRenamed("COD_VENDA", "id_transacao")
        .withColumnRenamed("LOJA", "contexto")
        .join(
            dim_por_legado,
            F.col("COD_PRODUTO") == F.col("codigo_legado_loja_fisica"),
            how="left"
        )
        .withColumn("sku", F.col("sku_ref"))
        .withColumn("fonte", F.lit("loja_fisica"))
        .select("id_transacao", "data", "sku", "categoria", "nome_produto",
                "quantidade", "preco_unitario", "fonte", "contexto", "_ingerido_em")
    )


def _padronizar_marketplace(spark: SparkSession, dim: DataFrame) -> DataFrame:
    bronze = spark.read.parquet(str(config.BRONZE_MARKETPLACE))
    # cada pedido tem um array `items` — explode transforma 1 linha "pedido
    # com N itens" em N linhas "item de pedido", que é o grão que queremos
    # (mesmo grão das outras duas fontes: 1 linha = 1 produto vendido)
    itens = bronze.withColumn("item", F.explode("items"))
    return (
        itens
        .withColumn("data", F.to_date(F.from_unixtime("timestamp")))
        .withColumn("sku", F.col("item.sku"))
        .withColumn("quantidade", F.col("item.qty").cast("int"))
        .withColumn("preco_unitario", F.col("item.price").cast("double"))
        .withColumnRenamed("orderId", "id_transacao")
        .withColumnRenamed("marketplace", "contexto")
        .join(F.broadcast(dim), on="sku", how="left")
        .withColumn("fonte", F.lit("marketplace"))
        .select("id_transacao", "data", "sku", "categoria", "nome_produto",
                "quantidade", "preco_unitario", "fonte", "contexto", "_ingerido_em")
    )


def _aplicar_gate_qualidade(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Retorna (validos, rejeitados_com_motivo)."""
    df_com_motivo = df.withColumn(
        "motivo_rejeicao",
        F.when(F.col("data").isNull(), F.lit("data_invalida"))
         .when(F.col("preco_unitario").isNull(), F.lit("preco_unitario_nulo"))
         .when(F.col("categoria").isNull(), F.lit("produto_nao_encontrado_na_referencia"))
         .when((F.col("quantidade").isNull()) | (F.col("quantidade") <= 0), F.lit("quantidade_invalida"))
         .otherwise(F.lit(None))
    )
    validos = df_com_motivo.filter(F.col("motivo_rejeicao").isNull()).drop("motivo_rejeicao")
    rejeitados = df_com_motivo.filter(F.col("motivo_rejeicao").isNotNull())
    return validos, rejeitados


def _deduplicar(df: DataFrame) -> DataFrame:
    """Mantém uma única linha por (fonte, id_transacao, sku), de forma determinística."""
    janela = Window.partitionBy("fonte", "id_transacao", "sku").orderBy(
        F.col("_ingerido_em").desc(),
        F.col("quantidade").desc_nulls_last(),
        F.col("preco_unitario").desc_nulls_last(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(janela))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def executar(spark: SparkSession) -> dict:
    with CronometroEtapa() as cron:
        dim = _carregar_dim_produto(spark)

        online = _padronizar_loja_online(spark, dim)
        fisica = _padronizar_loja_fisica(spark, dim)
        marketplace = _padronizar_marketplace(spark, dim)

        unificado = online.unionByName(fisica).unionByName(marketplace)
        n_lidas = unificado.count()

        deduplicado = _deduplicar(unificado)
        n_deduplicado = deduplicado.count()
        n_duplicadas = n_lidas - n_deduplicado

        validos, rejeitados = _aplicar_gate_qualidade(deduplicado)
        validos = validos.withColumn("valor_total", F.round(F.col("quantidade") * F.col("preco_unitario"), 2))

        config.SILVER_DIR.mkdir(parents=True, exist_ok=True)
        validos.write.mode("overwrite").parquet(str(config.SILVER_VENDAS))
        rejeitados.write.mode("overwrite").parquet(str(config.SILVER_QUARENTENA))

        n_validos = validos.count()
        n_rejeitados = rejeitados.count()

        # RECONCILIAÇÃO: nenhuma linha pode "sumir" no caminho
        if n_lidas != n_validos + n_rejeitados + n_duplicadas:
            raise AssertionError(
                f"Reconciliação falhou: lidas={n_lidas} != válidas={n_validos} "
                f"+ rejeitadas={n_rejeitados} + duplicadas={n_duplicadas}")

    registrar_execucao("silver", "vendas_unificadas", n_lidas, n_validos, n_rejeitados,
                        cron.duracao_segundos, "sucesso", linhas_duplicadas=n_duplicadas)

    print(f"[silver] {n_lidas} lidas = {n_validos} válidas + {n_rejeitados} em quarentena "
          f"+ {n_duplicadas} duplicadas")
    if n_rejeitados > 0:
        print("[silver] motivos de rejeição:")
        rejeitados.groupBy("motivo_rejeicao").count().show(truncate=False)

    return {"lidas": n_lidas, "validas": n_validos, "rejeitadas": n_rejeitados,
            "duplicadas": n_duplicadas}


if __name__ == "__main__":
    from spark_session import get_spark
    spark = get_spark()
    executar(spark)
    spark.stop()
