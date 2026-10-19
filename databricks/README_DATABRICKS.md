# Rodando este pipeline no Databricks

> **Status:** o pipeline foi **executado e testado localmente** (PySpark 3.5.1, Java 17).
> A adaptação para Databricks descrita aqui **ainda não foi executada** em um workspace.

## O que muda (e o que não muda)

| Aspecto | Local (este projeto) | Databricks |
|---|---|---|
| Sessão Spark | `get_spark()` (`local[*]`) | variável `spark` já existe no notebook |
| Armazenamento | Parquet em `data/` | **Delta** em tabelas do Unity Catalog / Volumes |
| Arquivos de entrada | `data/raw/...` | **Volume** (`/Volumes/<catalogo>/<esquema>/<volume>/raw/...`) |
| Controle incremental | CSV com hash MD5 | **Auto Loader** (`cloudFiles`) com checkpoint, ou tabela de controle |
| Silver | recalculada por inteiro | `MERGE INTO` incremental (Delta) |
| Orquestração | `orchestrator.py` | **Workflows/Jobs** (uma task por camada) |
| Regras de negócio | iguais | **iguais** (gate de qualidade, de-para, dedup) |

As transformações (`_padronizar_*`, `_aplicar_gate_qualidade`, `_deduplicar`)
são as mesmas: só mudam leitura, escrita e agendamento.

## Passo a passo sugerido

1. **Workspace:** use o Databricks da empresa ou o *Databricks Free Edition*.
2. **Dados de entrada:** crie um *Volume* e envie `data/raw/` e
   `data/referencia/dim_produto.csv`.
3. **Notebook:** crie um notebook Python e copie o conteúdo de `src/` em células
   (config → ingest → transform → aggregate). Remova `get_spark()` e use `spark`.
4. **Caminhos:** em `config.py`, troque `BASE_DIR` pelo caminho do Volume
   (`/Volumes/...`).
5. **Escrita em Delta:** troque
   `df.write.mode("overwrite").parquet(path)` por
   `df.write.format("delta").mode("overwrite").saveAsTable("catalogo.esquema.tabela")`.
6. **Silver incremental com MERGE** (evolução):

```python
from delta.tables import DeltaTable

alvo = DeltaTable.forName(spark, "catalogo.esquema.silver_vendas")
(alvo.alias("t")
     .merge(novos.alias("s"),
            "t.fonte = s.fonte AND t.id_transacao = s.id_transacao AND t.sku = s.sku")
     .whenMatchedUpdateAll(condition="s._ingerido_em > t._ingerido_em")
     .whenNotMatchedInsertAll()
     .execute())
```

7. **Ingestão com Auto Loader** (substitui o controle por hash):

```python
(spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", True)
      .option("cloudFiles.schemaLocation", "/Volumes/.../_schemas/loja_online")
      .load("/Volumes/.../raw/loja_online")
      .writeStream
      .option("checkpointLocation", "/Volumes/.../_checkpoints/bronze_loja_online")
      .trigger(availableNow=True)
      .toTable("catalogo.esquema.bronze_loja_online"))
```

8. **Workflow:** crie um Job com 3 tasks (bronze → silver → gold), dependências
   entre elas, *retries* e alerta de falha.
