# Pipeline de Ingestão e Transformação de Dados de Vendas

Projeto pessoal de engenharia de dados, construído para praticar e
demonstrar a arquitetura medalhão (**Bronze → Silver → Gold**) usando
**PySpark**, com carga incremental, integração de fontes heterogêneas e
gate de qualidade de dados.

> Projeto de estudo com dados fictícios gerados por mim: não é um projeto de nenhuma
> empresa.

---

## Status (verificado em 11/10/2026)

| Item | Resultado |
|---|---|
| Execução completa (Bronze → Silver → Gold) | **Executada com sucesso** (PySpark 3.5.1, Java 17, Python 3.12, Windows 11) |
| Carga incremental | 2ª execução: `sem_novidade` nas 3 fontes; Silver idêntica |
| Testes automatizados | **3 testes passam** (2 de regras + 1 ponta a ponta) |
| Reconciliação | `729 lidas = 719 válidas + 8 quarentena + 2 duplicadas` |
| Databricks | **Não executado** (adaptação documentada em `databricks/README_DATABRICKS.md`) |

### Correções feitas na revisão
1. **Data inválida** agora vai para a quarentena (`data_invalida`); antes passava com data nula e gerava mês nulo no Gold.
2. **Reconciliação**: o número de duplicatas é contado e a conta é verificada (o pipeline interrompe se não fechar).
3. **Deduplicação determinística** por *window function* (mais recente por `_ingerido_em`), no lugar de `dropDuplicates`.
4. **Testes automatizados** (`tests/`), incluindo o teste ponta a ponta de carga incremental.
5. Bronze contava linhas duas vezes por arquivo; agora conta uma vez.
6. `spark.sql.shuffle.partitions = 4` (execução local bem mais rápida) e exportador de amostras em CSV.

---

## 1. O problema (cenário simulado)

Uma loja fictícia de artigos esportivos ("Vitrine Esportiva") vende por
3 canais, e cada canal exporta os dados de um jeito diferente:

| Fonte | Formato | Particularidades |
|---|---|---|
| Loja Online (e-commerce) | CSV, vírgula | Datas ISO, SKU padrão |
| Loja Física (PDV) | CSV, ponto-e-vírgula | Decimal com vírgula, datas BR (`dd/mm/aaaa`), usa um **código de produto legado** (não o SKU) |
| Marketplace | JSON Lines | Timestamp Unix, pedido com **múltiplos itens aninhados** |

Sem um pipeline, alguém precisaria abrir os 3 arquivos, converter formatos
manualmente, descobrir a que produto cada código legado corresponde, tirar
duplicatas e só então conseguir somar "quanto vendemos, por categoria, no
mês" — um processo lento e sujeito a erro humano.

## 2. A solução — arquitetura medalhão

```
data/raw/  ──▶  BRONZE  ──▶  SILVER  ──▶  GOLD
(3 formatos)   (dado bruto    (schema      (agregados de
                + metadados    único,       negócio, prontos
                de ingestão)   limpo,       para BI/consumo)
                               íntegro)
```

- **Bronze** (`src/ingest_bronze.py`): ingestão bruta, um formato por
  fonte, sem transformação de conteúdo — só adiciona metadados
  (`_fonte_sistema`, `_arquivo_origem`, `_ingerido_em`). Ingestão é
  **incremental**: um arquivo de controle (`control/arquivos_processados.csv`)
  guarda o hash de cada arquivo já ingerido; arquivos novos ou alterados são
  processados, os demais são pulados.

- **Silver** (`src/transform_silver.py`): as 3 fontes viram um único
  schema (`id_transacao, data, sku, categoria, nome_produto, quantidade,
  preco_unitario, valor_total, fonte`). Aqui acontece a parte mais
  importante do projeto:
  - **Integração via tabela de referência** (`data/referencia/dim_produto.csv`):
    a loja física usa um código de produto próprio (legado); a tabela de
    referência faz a ponte entre esse código e o SKU canônico.
  - **Gate de qualidade**: registros com preço nulo, quantidade inválida ou
    produto não encontrado na referência vão para uma tabela de
    **quarentena** (`silver/vendas_quarentena`), com o motivo da rejeição —
    em vez de contaminar as métricas ou serem descartados silenciosamente.
  - **Deduplicação**: remove registros duplicados exatos (reenvio da
    origem).

- **Gold** (`src/aggregate_gold.py`): duas tabelas de consumo —
  `vendas_mensal_categoria` (receita/quantidade por mês, categoria e canal)
  e `top_produtos` (ranking de produtos por receita).

- **Orquestração** (`src/orchestrator.py`): roda as 3 camadas em sequência,
  registrando cada execução em `control/pipeline_execution_log.csv`
  (linhas lidas/gravadas/rejeitadas, duração, status) — é o que permite
  **monitorar** o pipeline.

### Por que Bronze é incremental, mas Silver/Gold recalculam tudo?

Decisão deliberada, e é importante saber explicar isso: a cada execução,
o Bronze só ingere arquivos novos (evita reprocessar histórico enorme na
ponta mais custosa, a leitura de arquivos brutos). Já Silver e Gold releem
o Bronze inteiro e recalculam do zero a cada execução — porque, no volume
deste projeto, isso é rápido e elimina qualquer risco de inconsistência.
Num pipeline em escala maior, o próximo passo evolutivo seria aplicar
`MERGE` incremental também no Silver (ex: com Delta Lake, disponível
nativamente no Databricks) — deixei essa evolução comentada no código
(`transform_silver.py`, função `_deduplicar`).

## 3. Estrutura do projeto

```
pipeline-vendas-medalhao/
├── data/
│   ├── raw/                    ← as 3 fontes brutas (entrada)
│   ├── referencia/
│   │   └── dim_produto.csv     ← tabela de integração SKU ↔ código legado
│   ├── bronze/                 ← gerado ao rodar o pipeline
│   ├── silver/                 ← gerado ao rodar o pipeline
│   └── gold/                   ← gerado ao rodar o pipeline
├── control/                    ← gerado ao rodar o pipeline (log + controle incremental)
├── src/
│   ├── config.py                caminhos centrais
│   ├── spark_session.py         criação da SparkSession
│   ├── utils_logging.py         controle incremental + log de execução
│   ├── ingest_bronze.py         camada Bronze
│   ├── transform_silver.py      camada Silver
│   ├── aggregate_gold.py        camada Gold
│   └── orchestrator.py          roda as 3 camadas em sequência
├── scripts/
│   ├── gerar_dados_exemplo.py   gera os dados brutos de exemplo (já rodado)
│   └── exportar_amostras.py     exporta Silver/Gold/log para CSV (abre no Excel)
├── tests/
│   └── test_pipeline.py         testes automatizados (unittest)
├── exemplo_saida/              resultado da execução de referência (CSV)
├── databricks/
│   └── README_DATABRICKS.md     como adaptar para o Databricks
├── requirements.txt
└── README.md                    (este arquivo)
```

---

## 4. Como rodar (PySpark local — sem precisar de Databricks)

Apache Spark é open-source: dá para rodar exatamente o mesmo código
localmente, sem custo e sem depender de nenhuma plataforma de nuvem.

### Pré-requisitos
- **Python 3.9 a 3.12**
- **Java 17** (recomendado) ou **Java 11** — o Spark roda sobre a JVM.
  - Verifique com `java -version`. Se não tiver, instale o
    [Eclipse Temurin JDK 17](https://adoptium.net/).

### Passo a passo

```bash
# 1. Crie e ative um ambiente virtual (recomendado)
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 2. Instale as dependências
pip install -r requirements.txt

# 3. (Os dados de exemplo já vêm gerados em data/raw/. Só rode este passo
#    se quiser regenerá-los ou gerar um volume diferente.)
python scripts/gerar_dados_exemplo.py

# 4. Rode o pipeline completo (Bronze -> Silver -> Gold)
cd src
python orchestrator.py
```

Ao final, você verá no terminal os totais de cada camada e as tabelas Gold
impressas. Para inspecionar os resultados depois, você pode abrir os
Parquets com o próprio PySpark (`spark.read.parquet(...)`) ou, mais simples,
com pandas: `pandas.read_parquet("data/gold/vendas_mensal_categoria")`.

### Demonstrando a carga incremental na prática

```bash
# Rode o orquestrador uma segunda vez, sem alterar nada:
python orchestrator.py
```

Você verá, na etapa Bronze, a mensagem `nada novo para processar` para
cada fonte — o controle por hash reconheceu que os arquivos já foram
ingeridos e pulou o reprocessamento. Para ver a ingestão incremental
"pegando" um dado novo de verdade, copie um dos arquivos de
`data/raw/loja_online/`, renomeie para simular um novo mês (ex:
`vendas_online_2026-03.csv`) e rode o orquestrador de novo — só esse
arquivo novo será ingerido no Bronze, e Silver/Gold são recalculados com
o dado adicional.

---

### Observações para Windows (importante)

- **`HADOOP_HOME`**: o Spark no Windows precisa de `winutils.exe` e `hadoop.dll` para gravar
  Parquet. Sem eles aparece `HADOOP_HOME and hadoop.home.dir are unset`. Baixe os dois
  arquivos de uma versão compatível com o Hadoop 3.3 (repositório *winutils*), coloque em
  `C:\hadoop\bin` e defina `HADOOP_HOME=C:\hadoop`.
- **Versões**: Python 3.9 a 3.12 (o PySpark 3.5.1 **não** suporta 3.13/3.14) e Java 11 ou 17.
- **Caminho curto**: instale o ambiente virtual em uma pasta de caminho curto
  (ex.: `C:\proj`); caminhos muito longos quebram a instalação no Windows.
- **Computador corporativo**: instalar Java costuma exigir administrador.

### Testes automatizados

```bash
python -m unittest discover -s tests -v
```

| Teste | O que prova |
|---|---|
| `test_cada_regra_envia_para_quarentena` | Cada regra do gate (data, preço, produto, quantidade) isola o registro |
| `test_deduplicacao_mantem_a_versao_mais_recente_e_e_deterministica` | A versão mais recente vence e o resultado é sempre o mesmo |
| `test_incremental_idempotente_e_reconciliacao` | 1ª carga ingere 3 fontes; 2ª não reprocessa; arquivo repetido gera duplicatas contadas; a conta fecha |

### Resultado de referência (execução de 11/10/2026)

| Camada | Valor |
|---|---|
| Bronze | 264 (loja online) + 194 (loja física) + 132 pedidos (marketplace) |
| Silver | 729 itens lidos → **719 válidos**, **8 em quarentena**, **2 duplicados** |
| Quarentena | 4 produto fora da referência · 2 preço nulo · 2 quantidade inválida |
| Gold | 30 linhas mensais por categoria/canal; 15 produtos ranqueados |
| Receita (Silver) | R$ 214.324,84 (loja física 80.166,22 · online 72.149,24 · marketplace 62.009,38) |

Os arquivos conferíveis estão em `exemplo_saida/` (CSV).

---

## 5. Alternativa: rodando no Databricks de verdade (gratuito)

A antiga "Community Edition" do Databricks foi descontinuada e substituída
pelo **Databricks Free Edition** — gratuito e permanente para uso pessoal
(estudo, portfólio, aprendizado). Se quiser rodar este mesmo projeto dentro
do Databricks real (o ambiente usado em muitas empresas), o caminho é:

1. Acesse a página de cadastro do **Databricks Free Edition**
   (busque por "Databricks Free Edition sign up" — o link muda de vez em
   quando, então prefira buscar em vez de confiar num link fixo aqui).
2. Crie um workspace pessoal gratuito.
3. Crie um Notebook novo e cole o conteúdo de cada arquivo de `src/`
   em células separadas — **removendo** a criação manual da SparkSession
   (`spark_session.py`): no Databricks, a variável `spark` já vem pronta
   no notebook, então é só usá-la diretamente.
4. Faça upload dos arquivos de `data/raw/` e `data/referencia/` para o
   DBFS (File → Upload Data, ou o utilitário `dbutils.fs`) e ajuste os
   caminhos em `config.py` para os caminhos do DBFS (`/FileStore/...`).

---

## Autora

Vivian Ribeiro · [GitHub](https://github.com/ribeirovivi)
