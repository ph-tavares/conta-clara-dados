# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze (Auto Loader)
# MAGIC Carga incremental do Volume `chegada`: uma linha por arquivo, com o JSON bruto. O checkpoint garante que cada arquivo entra uma vez só.

# COMMAND ----------

from pyspark.sql import functions as F

ESQUEMA = "workspace.conta_clara"
BASE = "/Volumes/workspace/conta_clara/chegada"

# campos de primeiro nivel; os blocos aninhados ficam como texto e sao tratados na Prata
FONTES = {
    "clima": "latitude DOUBLE, longitude DOUBLE, timezone STRING, hourly_units STRING, hourly STRING",
    "aneel": "dia_referencia STRING, distribuidora STRING, cnpj STRING, tarifas STRING, "
             "bandeira_acionamento STRING, bandeira_adicional STRING",
    "leituras": "dia_referencia STRING, origem STRING, aviso STRING, familias STRING, leituras STRING, contas STRING",
}
BLOCO = {"clima": "hourly", "aneel": "tarifas", "leituras": "leituras"}

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ESQUEMA}.bronze_controle_cargas (
  executado_em TIMESTAMP, tabela STRING, linhas_antes BIGINT, linhas_depois BIGINT, arquivos_novos BIGINT
) COMMENT 'Historico das cargas da Bronze'
""")


def contar(tabela: str) -> int:
    if not spark.catalog.tableExists(tabela):
        return 0
    return spark.table(tabela).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga

# COMMAND ----------

resultado = []
for fonte, campos in FONTES.items():
    tabela = f"{ESQUEMA}.bronze_{fonte}"
    antes = contar(tabela)

    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .schema(campos)
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("data_referencia",
                    F.to_date(F.regexp_extract("arquivo_origem", r"(\d{4}-\d{2}-\d{2})\.json$", 1)))
        .withColumn("carregado_em", F.current_timestamp())
     .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)
        .toTable(tabela)
        .awaitTermination())

    depois = contar(tabela)
    resultado.append((tabela.split(".")[-1], antes, depois, depois - antes))

spark.createDataFrame(resultado, "tabela STRING, linhas_antes BIGINT, linhas_depois BIGINT, arquivos_novos BIGINT") \
    .withColumn("executado_em", F.current_timestamp()) \
    .select("executado_em", "tabela", "linhas_antes", "linhas_depois", "arquivos_novos") \
    .write.mode("append").saveAsTable(f"{ESQUEMA}.bronze_controle_cargas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Histórico das cargas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dense_rank() OVER (ORDER BY executado_em) AS execucao,
# MAGIC        date_format(from_utc_timestamp(executado_em, 'America/Sao_Paulo'), 'dd/MM HH:mm:ss') AS quando,
# MAGIC        tabela, linhas_antes, linhas_depois, arquivos_novos
# MAGIC FROM workspace.conta_clara.bronze_controle_cargas
# MAGIC ORDER BY executado_em, tabela;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquivos carregados por dia de referência

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT data_referencia,
# MAGIC        sum(CASE WHEN fonte = 'clima' THEN n END)    AS clima,
# MAGIC        sum(CASE WHEN fonte = 'aneel' THEN n END)    AS aneel,
# MAGIC        sum(CASE WHEN fonte = 'leituras' THEN n END) AS leituras
# MAGIC FROM (
# MAGIC   SELECT 'clima' AS fonte, data_referencia, count(*) AS n FROM workspace.conta_clara.bronze_clima GROUP BY ALL
# MAGIC   UNION ALL SELECT 'aneel', data_referencia, count(*) FROM workspace.conta_clara.bronze_aneel GROUP BY ALL
# MAGIC   UNION ALL SELECT 'leituras', data_referencia, count(*) FROM workspace.conta_clara.bronze_leituras GROUP BY ALL
# MAGIC )
# MAGIC GROUP BY data_referencia ORDER BY data_referencia;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

vazios = {fonte: spark.table(f"{ESQUEMA}.bronze_{fonte}").filter(f"{bloco} IS NULL").count() for fonte, bloco in BLOCO.items()}
repetidos = {fonte: spark.table(f"{ESQUEMA}.bronze_{fonte}").groupBy("arquivo_origem").count().filter("count > 1").count()
             for fonte in FONTES}
assert not any(vazios.values()), f"Arquivos sem o bloco de registros na Bronze: {vazios}"
assert not any(repetidos.values()), f"Arquivo carregado mais de uma vez: {repetidos}"
print("Bronze conferida: todo arquivo traz o bloco de registros e nenhum arquivo entrou duas vezes.")
