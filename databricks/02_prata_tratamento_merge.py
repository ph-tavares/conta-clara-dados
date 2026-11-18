# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Prata
# MAGIC Tipos, nulos e domínios tratados, regras de qualidade e MERGE pela chave.
# MAGIC
# MAGIC | Regra | Fonte | O que barra |
# MAGIC |---|---|---|
# MAGIC | Q1 | Leituras | leitura de família que não está cadastrada no piloto |
# MAGIC | Q2 | Leituras | número do medidor nulo, negativo ou acima de 999.999, ou sem data |
# MAGIC | Q3 | Leituras | foto lida com confiança abaixo de 0,70 que o cidadão não confirmou |
# MAGIC | Q4 | Contas | conta sem kWh, com kWh ≤ 0, período fora de 1 a 60 dias ou de família não cadastrada |
# MAGIC | Q5 | Clima, ANEEL | medida externa inválida: temperatura fora de -5 °C a 45 °C, chuva negativa, tarifa sem valor ou fora de MWh |
# MAGIC | Q6 | Todas | mesma chave repetida no arquivo ou entre lotes: fica a versão mais recente (a correção vence) |
# MAGIC
# MAGIC Os registros barrados por Q1 a Q5 vão para `prata_rejeitados`.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE workspace.conta_clara;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_familia (
# MAGIC   id_familia STRING, nome_responsavel STRING, telefone STRING COMMENT 'ficticio; nao vai para a Ouro',
# MAGIC   bairro STRING, moradores INT, tarifa_social BOOLEAN, cadastrada_em DATE, origem STRING,
# MAGIC   data_referencia DATE, atualizado_na_prata TIMESTAMP
# MAGIC ) COMMENT 'Cadastro das familias do piloto (demonstracao). Chave: id_familia';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_leitura (
# MAGIC   id_familia STRING, data_leitura DATE, kwh_medidor BIGINT,
# MAGIC   origem_leitura STRING COMMENT 'foto ou digitada', confianca DOUBLE, confirmada_pelo_cidadao BOOLEAN,
# MAGIC   correcao BOOLEAN COMMENT 'reenvio que corrige uma leitura de dia anterior',
# MAGIC   data_referencia DATE, atualizado_na_prata TIMESTAMP
# MAGIC ) COMMENT 'Leituras do medidor enviadas pelas familias. Chave: (id_familia, data_leitura)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_conta (
# MAGIC   id_conta STRING, id_familia STRING, competencia STRING, data_leitura_distribuidora DATE,
# MAGIC   dias_faturados INT, kwh_cobrado INT, valor_cobrado DOUBLE,
# MAGIC   tipo_faturamento STRING COMMENT 'leitura ou media (estimada pela distribuidora)',
# MAGIC   data_referencia DATE, atualizado_na_prata TIMESTAMP
# MAGIC ) COMMENT 'Contas de luz informadas pelas familias. Chave: id_conta';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_clima_hora (
# MAGIC   data_hora TIMESTAMP COMMENT 'hora local de Sao Paulo', data DATE,
# MAGIC   temperatura_c DOUBLE, sensacao_c DOUBLE, precipitacao_mm DOUBLE,
# MAGIC   data_referencia DATE, atualizado_na_prata TIMESTAMP
# MAGIC ) COMMENT 'Clima hora a hora no piloto (Open-Meteo). Chave: data_hora';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_tarifa (
# MAGIC   distribuidora STRING, subclasse STRING, detalhe STRING, inicio_vigencia DATE, fim_vigencia DATE,
# MAGIC   resolucao STRING, tusd_rs_mwh DOUBLE, te_rs_mwh DOUBLE, tarifa_rs_kwh DOUBLE COMMENT '(TUSD + TE) / 1000, sem impostos',
# MAGIC   data_referencia DATE, atualizado_na_prata TIMESTAMP
# MAGIC ) COMMENT 'Tarifas homologadas B1 residencial da distribuidora (ANEEL). Chave: (subclasse, detalhe, inicio_vigencia)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_bandeira (
# MAGIC   competencia DATE, bandeira STRING, adicional_rs_mwh DOUBLE,
# MAGIC   data_referencia DATE, atualizado_na_prata TIMESTAMP
# MAGIC ) COMMENT 'Bandeira tarifaria acionada no mes (ANEEL). Chave: competencia';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_qualidade (
# MAGIC   executado_em TIMESTAMP, fonte STRING, regra STRING, descricao STRING,
# MAGIC   total_avaliado BIGINT, total_barrado BIGINT
# MAGIC ) COMMENT 'Resultado das regras de qualidade a cada execucao da Prata';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cadastro das famílias

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW familias_tipado AS
# MAGIC SELECT trim(f.id_familia) AS id_familia, trim(f.nome_responsavel) AS nome_responsavel, trim(f.telefone) AS telefone,
# MAGIC        initcap(trim(f.bairro)) AS bairro, CAST(f.moradores AS INT) AS moradores,
# MAGIC        coalesce(f.tarifa_social, false) AS tarifa_social, try_cast(f.cadastrada_em AS DATE) AS cadastrada_em,
# MAGIC        coalesce(b.origem, 'cidadao') AS origem, b.data_referencia, b.arquivo_origem
# MAGIC FROM workspace.conta_clara.bronze_leituras b
# MAGIC LATERAL VIEW explode(from_json(b.familias,
# MAGIC   'ARRAY<STRUCT<id_familia: STRING, nome_responsavel: STRING, telefone: STRING, bairro: STRING, moradores: INT, tarifa_social: BOOLEAN, cadastrada_em: STRING>>'
# MAGIC )) t AS f
# MAGIC WHERE f.id_familia IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW lote_prata_familia AS
# MAGIC SELECT id_familia, nome_responsavel, telefone, bairro, moradores, tarifa_social, cadastrada_em, origem,
# MAGIC        data_referencia, current_timestamp() AS atualizado_na_prata
# MAGIC FROM familias_tipado
# MAGIC QUALIFY row_number() OVER (PARTITION BY id_familia ORDER BY data_referencia DESC) = 1;
# MAGIC
# MAGIC -- familias conhecidas: as que ja estao na Prata e as que chegam neste lote
# MAGIC CREATE OR REPLACE TEMP VIEW familias_cadastradas AS
# MAGIC SELECT id_familia FROM lote_prata_familia UNION SELECT id_familia FROM workspace.conta_clara.prata_familia;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leituras do medidor

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW leituras_tipado AS
# MAGIC SELECT trim(l.id_familia) AS id_familia, try_cast(l.data_leitura AS DATE) AS data_leitura,
# MAGIC        CAST(l.kwh_medidor AS BIGINT) AS kwh_medidor, lower(trim(l.origem_leitura)) AS origem_leitura,
# MAGIC        l.confianca, coalesce(l.confirmada_pelo_cidadao, false) AS confirmada_pelo_cidadao,
# MAGIC        coalesce(l.correcao, false) AS correcao, b.data_referencia, b.arquivo_origem
# MAGIC FROM workspace.conta_clara.bronze_leituras b
# MAGIC LATERAL VIEW explode(from_json(b.leituras,
# MAGIC   'ARRAY<STRUCT<id_familia: STRING, data_leitura: STRING, kwh_medidor: BIGINT, origem_leitura: STRING, confianca: DOUBLE, confirmada_pelo_cidadao: BOOLEAN, correcao: BOOLEAN>>'
# MAGIC )) t AS l;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW leituras_avaliado AS
# MAGIC SELECT l.*,
# MAGIC   CASE WHEN c.id_familia IS NULL THEN 'Q1_leitura_familia_inexistente'
# MAGIC        WHEN l.data_leitura IS NULL OR l.kwh_medidor IS NULL OR l.kwh_medidor < 0 OR l.kwh_medidor > 999999
# MAGIC          THEN 'Q2_leitura_valor_invalido'
# MAGIC        WHEN l.origem_leitura = 'foto' AND coalesce(l.confianca, 0) < 0.70 AND NOT l.confirmada_pelo_cidadao
# MAGIC          THEN 'Q3_leitura_foto_nao_confirmada'
# MAGIC   END AS regra_barrada
# MAGIC FROM leituras_tipado l
# MAGIC LEFT JOIN familias_cadastradas c ON c.id_familia = l.id_familia;
# MAGIC
# MAGIC -- Q6: duplicadas saem; a correcao enviada depois vence a leitura original
# MAGIC CREATE OR REPLACE TEMP VIEW lote_prata_leitura AS
# MAGIC SELECT id_familia, data_leitura, kwh_medidor, origem_leitura, confianca, confirmada_pelo_cidadao, correcao,
# MAGIC        data_referencia, current_timestamp() AS atualizado_na_prata
# MAGIC FROM leituras_avaliado
# MAGIC WHERE regra_barrada IS NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY id_familia, data_leitura ORDER BY data_referencia DESC, correcao DESC) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contas de luz

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW contas_tipado AS
# MAGIC SELECT trim(c.id_conta) AS id_conta, trim(c.id_familia) AS id_familia, c.competencia,
# MAGIC        try_cast(c.data_leitura_distribuidora AS DATE) AS data_leitura_distribuidora,
# MAGIC        CAST(c.dias_faturados AS INT) AS dias_faturados, CAST(c.kwh_cobrado AS INT) AS kwh_cobrado,
# MAGIC        CAST(c.valor_cobrado AS DOUBLE) AS valor_cobrado, lower(trim(c.tipo_faturamento)) AS tipo_faturamento,
# MAGIC        b.data_referencia, b.arquivo_origem
# MAGIC FROM workspace.conta_clara.bronze_leituras b
# MAGIC LATERAL VIEW explode(from_json(b.contas,
# MAGIC   'ARRAY<STRUCT<id_conta: STRING, id_familia: STRING, competencia: STRING, data_leitura_distribuidora: STRING, dias_faturados: INT, kwh_cobrado: INT, valor_cobrado: DOUBLE, tipo_faturamento: STRING>>'
# MAGIC )) t AS c;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW contas_avaliado AS
# MAGIC SELECT c.*,
# MAGIC   CASE WHEN c.id_conta IS NULL OR f.id_familia IS NULL OR c.data_leitura_distribuidora IS NULL
# MAGIC          OR c.kwh_cobrado IS NULL OR c.kwh_cobrado <= 0
# MAGIC          OR c.dias_faturados IS NULL OR c.dias_faturados NOT BETWEEN 1 AND 60
# MAGIC          OR c.tipo_faturamento NOT IN ('leitura', 'media') THEN 'Q4_conta_invalida'
# MAGIC   END AS regra_barrada
# MAGIC FROM contas_tipado c
# MAGIC LEFT JOIN familias_cadastradas f ON f.id_familia = c.id_familia;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW lote_prata_conta AS
# MAGIC SELECT id_conta, id_familia, competencia, data_leitura_distribuidora, dias_faturados, kwh_cobrado, valor_cobrado,
# MAGIC        tipo_faturamento, data_referencia, current_timestamp() AS atualizado_na_prata
# MAGIC FROM contas_avaliado
# MAGIC WHERE regra_barrada IS NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY id_conta ORDER BY data_referencia DESC) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Clima (Open-Meteo)
# MAGIC Listas paralelas: `arrays_zip` + `explode`, uma linha por hora.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW clima_tipado AS
# MAGIC WITH listas AS (
# MAGIC   SELECT h.time AS horas, h.temperature_2m AS temperaturas, h.apparent_temperature AS sensacoes,
# MAGIC          h.precipitation AS chuvas, data_referencia, arquivo_origem
# MAGIC   FROM (SELECT from_json(hourly, 'STRUCT<time: ARRAY<STRING>, temperature_2m: ARRAY<DOUBLE>, apparent_temperature: ARRAY<DOUBLE>, precipitation: ARRAY<DOUBLE>>') AS h,
# MAGIC                data_referencia, arquivo_origem
# MAGIC         FROM workspace.conta_clara.bronze_clima)
# MAGIC )
# MAGIC SELECT coalesce(try_cast(z.horas AS TIMESTAMP), try_cast(concat(z.horas, ':00') AS TIMESTAMP)) AS data_hora,
# MAGIC        z.temperaturas AS temperatura_c, z.sensacoes AS sensacao_c, z.chuvas AS precipitacao_mm,
# MAGIC        data_referencia, arquivo_origem
# MAGIC FROM listas
# MAGIC LATERAL VIEW explode(arrays_zip(horas, temperaturas, sensacoes, chuvas)) t AS z;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW clima_avaliado AS
# MAGIC SELECT *,
# MAGIC   CASE WHEN data_hora IS NULL OR precipitacao_mm IS NULL OR precipitacao_mm < 0
# MAGIC          OR temperatura_c IS NULL OR temperatura_c NOT BETWEEN -5 AND 45 THEN 'Q5_medida_externa_invalida'
# MAGIC   END AS regra_barrada
# MAGIC FROM clima_tipado;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW lote_prata_clima AS
# MAGIC SELECT data_hora, CAST(data_hora AS DATE) AS data, temperatura_c, sensacao_c, precipitacao_mm, data_referencia,
# MAGIC        current_timestamp() AS atualizado_na_prata
# MAGIC FROM clima_avaliado
# MAGIC WHERE regra_barrada IS NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY data_hora ORDER BY data_referencia DESC, arquivo_origem DESC) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tarifa e bandeira (ANEEL)
# MAGIC Valores vêm como texto com vírgula decimal (`"472,42"`, `",00"`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION numero_br(v STRING) RETURNS DOUBLE
# MAGIC RETURN try_cast(replace(replace(trim(v), '.', ''), ',', '.') AS DOUBLE);
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW tarifas_tipado AS
# MAGIC SELECT b.distribuidora, t.DscSubClasse AS subclasse, t.DscDetalhe AS detalhe,
# MAGIC        try_cast(t.DatInicioVigencia AS DATE) AS inicio_vigencia, try_cast(t.DatFimVigencia AS DATE) AS fim_vigencia,
# MAGIC        t.DscREH AS resolucao, t.DscUnidadeTerciaria AS unidade,
# MAGIC        numero_br(t.VlrTUSD) AS tusd_rs_mwh, numero_br(t.VlrTE) AS te_rs_mwh, b.data_referencia, b.arquivo_origem
# MAGIC FROM workspace.conta_clara.bronze_aneel b
# MAGIC LATERAL VIEW explode(from_json(b.tarifas,
# MAGIC   'ARRAY<STRUCT<DscREH: STRING, DatInicioVigencia: STRING, DatFimVigencia: STRING, DscSubClasse: STRING, DscDetalhe: STRING, DscUnidadeTerciaria: STRING, VlrTUSD: STRING, VlrTE: STRING>>'
# MAGIC )) x AS t;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW tarifas_avaliado AS
# MAGIC SELECT *,
# MAGIC   CASE WHEN inicio_vigencia IS NULL OR tusd_rs_mwh IS NULL OR te_rs_mwh IS NULL OR tusd_rs_mwh < 0 OR te_rs_mwh < 0
# MAGIC          OR unidade <> 'MWh' THEN 'Q5_medida_externa_invalida' END AS regra_barrada
# MAGIC FROM tarifas_tipado;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW lote_prata_tarifa AS
# MAGIC SELECT distribuidora, subclasse, detalhe, inicio_vigencia, fim_vigencia, resolucao, tusd_rs_mwh, te_rs_mwh,
# MAGIC        round((tusd_rs_mwh + te_rs_mwh) / 1000, 5) AS tarifa_rs_kwh, data_referencia,
# MAGIC        current_timestamp() AS atualizado_na_prata
# MAGIC FROM tarifas_avaliado
# MAGIC WHERE regra_barrada IS NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY subclasse, detalhe, inicio_vigencia ORDER BY data_referencia DESC) = 1;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW lote_prata_bandeira AS
# MAGIC SELECT try_cast(a.DatCompetencia AS DATE) AS competencia, initcap(trim(a.NomBandeiraAcionada)) AS bandeira,
# MAGIC        numero_br(a.VlrAdicionalBandeira) AS adicional_rs_mwh, b.data_referencia,
# MAGIC        current_timestamp() AS atualizado_na_prata
# MAGIC FROM workspace.conta_clara.bronze_aneel b
# MAGIC LATERAL VIEW explode(from_json(b.bandeira_acionamento,
# MAGIC   'ARRAY<STRUCT<DatCompetencia: STRING, NomBandeiraAcionada: STRING, VlrAdicionalBandeira: STRING>>'
# MAGIC )) x AS a
# MAGIC WHERE try_cast(a.DatCompetencia AS DATE) IS NOT NULL AND numero_br(a.VlrAdicionalBandeira) IS NOT NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY try_cast(a.DatCompetencia AS DATE) ORDER BY b.data_referencia DESC) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Regras de qualidade

# COMMAND ----------

REGRAS = [
    # fonte, regra, descricao, view avaliada, view sem duplicatas (None = regra de validade)
    ("leituras", "Q1_leitura_familia_inexistente", "Leitura de família não cadastrada no piloto", "leituras_avaliado", None),
    ("leituras", "Q2_leitura_valor_invalido", "Número do medidor nulo, negativo ou acima de 999.999, ou sem data", "leituras_avaliado", None),
    ("leituras", "Q3_leitura_foto_nao_confirmada", "Foto com confiança < 0,70 sem confirmação do cidadão", "leituras_avaliado", None),
    ("contas", "Q4_conta_invalida", "Conta sem kWh, kWh ≤ 0, período fora de 1 a 60 dias ou família não cadastrada", "contas_avaliado", None),
    ("clima", "Q5_medida_externa_invalida", "Temperatura fora de -5 a 45 °C, chuva negativa ou hora sem medição", "clima_avaliado", None),
    ("aneel", "Q5_medida_externa_invalida", "Tarifa sem valor de TUSD/TE ou em unidade diferente de MWh", "tarifas_avaliado", None),
    ("leituras", "Q6_duplicada_ou_corrigida", "Mesma chave repetida: fica a versão mais recente (correção vence)", "leituras_avaliado", "lote_prata_leitura"),
    ("contas", "Q6_duplicada_ou_corrigida", "Mesma chave repetida: fica a versão mais recente", "contas_avaliado", "lote_prata_conta"),
    ("clima", "Q6_duplicada_ou_corrigida", "Mesma chave repetida: fica a versão mais recente", "clima_avaliado", "lote_prata_clima"),
    ("aneel", "Q6_duplicada_ou_corrigida", "Mesma tarifa em vários dias: fica uma por vigência", "tarifas_avaliado", "lote_prata_tarifa"),
]

linhas = []
for fonte, regra, descricao, avaliada, lote in REGRAS:
    if lote is None:
        avaliado = spark.table(avaliada).count()
        barrado = spark.table(avaliada).filter(f"regra_barrada = '{regra}'").count()
    else:
        avaliado = spark.table(avaliada).filter("regra_barrada IS NULL").count()
        barrado = avaliado - spark.table(lote).count()
    linhas.append((fonte, regra, descricao, avaliado, barrado))

qualidade = spark.createDataFrame(
    linhas, "fonte STRING, regra STRING, descricao STRING, total_avaliado BIGINT, total_barrado BIGINT")
qualidade.selectExpr("current_timestamp() AS executado_em", "*") \
    .write.mode("append").saveAsTable("workspace.conta_clara.prata_qualidade")
display(qualidade.orderBy("regra", "fonte"))

# COMMAND ----------

# MAGIC %md
# MAGIC Quarentena

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.conta_clara.prata_rejeitados
# MAGIC COMMENT 'Registros barrados pelas regras Q1 a Q5' AS
# MAGIC SELECT 'leituras' AS fonte, regra_barrada AS regra, concat(id_familia, ' ', coalesce(CAST(data_leitura AS STRING), 'sem data')) AS chave,
# MAGIC        concat('kwh=', coalesce(CAST(kwh_medidor AS STRING), 'nulo'), ' confianca=', coalesce(CAST(confianca AS STRING), '-')) AS detalhe,
# MAGIC        data_referencia, arquivo_origem
# MAGIC FROM leituras_avaliado WHERE regra_barrada IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'contas', regra_barrada, coalesce(id_conta, 'sem id'),
# MAGIC        concat('kwh=', coalesce(CAST(kwh_cobrado AS STRING), 'nulo'), ' dias=', coalesce(CAST(dias_faturados AS STRING), 'nulo')),
# MAGIC        data_referencia, arquivo_origem
# MAGIC FROM contas_avaliado WHERE regra_barrada IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'clima', regra_barrada, CAST(data_hora AS STRING),
# MAGIC        concat('temp=', coalesce(CAST(temperatura_c AS STRING), 'nulo'), ' chuva=', coalesce(CAST(precipitacao_mm AS STRING), 'nulo')),
# MAGIC        data_referencia, arquivo_origem
# MAGIC FROM clima_avaliado WHERE regra_barrada IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'aneel', regra_barrada, concat(subclasse, ' ', coalesce(CAST(inicio_vigencia AS STRING), '')),
# MAGIC        concat('tusd=', coalesce(CAST(tusd_rs_mwh AS STRING), 'nulo'), ' te=', coalesce(CAST(te_rs_mwh AS STRING), 'nulo')),
# MAGIC        data_referencia, arquivo_origem
# MAGIC FROM tarifas_avaliado WHERE regra_barrada IS NOT NULL;
# MAGIC
# MAGIC SELECT fonte, regra, count(*) AS registros_em_quarentena FROM workspace.conta_clara.prata_rejeitados GROUP BY ALL ORDER BY regra;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. MERGE
# MAGIC Atualiza só quando o lote é mais recente que o gravado.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.conta_clara.prata_familia AS alvo
# MAGIC USING lote_prata_familia AS novo
# MAGIC ON alvo.id_familia = novo.id_familia
# MAGIC WHEN MATCHED AND novo.data_referencia > alvo.data_referencia THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.conta_clara.prata_leitura AS alvo
# MAGIC USING lote_prata_leitura AS novo
# MAGIC ON alvo.id_familia = novo.id_familia AND alvo.data_leitura = novo.data_leitura
# MAGIC WHEN MATCHED AND novo.data_referencia > alvo.data_referencia THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.conta_clara.prata_conta AS alvo
# MAGIC USING lote_prata_conta AS novo
# MAGIC ON alvo.id_conta = novo.id_conta
# MAGIC WHEN MATCHED AND novo.data_referencia > alvo.data_referencia THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.conta_clara.prata_clima_hora AS alvo
# MAGIC USING lote_prata_clima AS novo
# MAGIC ON alvo.data_hora = novo.data_hora
# MAGIC WHEN MATCHED AND novo.data_referencia > alvo.data_referencia THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.conta_clara.prata_tarifa AS alvo
# MAGIC USING lote_prata_tarifa AS novo
# MAGIC ON alvo.subclasse = novo.subclasse AND alvo.detalhe = novo.detalhe AND alvo.inicio_vigencia = novo.inicio_vigencia
# MAGIC WHEN MATCHED AND (novo.tusd_rs_mwh <> alvo.tusd_rs_mwh OR novo.te_rs_mwh <> alvo.te_rs_mwh) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.conta_clara.prata_bandeira AS alvo
# MAGIC USING lote_prata_bandeira AS novo
# MAGIC ON alvo.competencia = novo.competencia
# MAGIC WHEN MATCHED AND (novo.bandeira <> alvo.bandeira OR novo.adicional_rs_mwh <> alvo.adicional_rs_mwh) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Conferência

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'prata_familia' AS tabela, count(*) AS linhas, count(DISTINCT id_familia) AS chaves_distintas FROM workspace.conta_clara.prata_familia
# MAGIC UNION ALL SELECT 'prata_leitura', count(*), count(DISTINCT id_familia, data_leitura) FROM workspace.conta_clara.prata_leitura
# MAGIC UNION ALL SELECT 'prata_conta', count(*), count(DISTINCT id_conta) FROM workspace.conta_clara.prata_conta
# MAGIC UNION ALL SELECT 'prata_clima_hora', count(*), count(DISTINCT data_hora) FROM workspace.conta_clara.prata_clima_hora
# MAGIC UNION ALL SELECT 'prata_tarifa', count(*), count(DISTINCT subclasse, detalhe, inicio_vigencia) FROM workspace.conta_clara.prata_tarifa
# MAGIC UNION ALL SELECT 'prata_bandeira', count(*), count(DISTINCT competencia) FROM workspace.conta_clara.prata_bandeira;

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

TABELAS_PRATA = ["prata_familia", "prata_leitura", "prata_conta", "prata_clima_hora", "prata_tarifa", "prata_bandeira"]
historicos = [
    spark.sql(f"DESCRIBE HISTORY workspace.conta_clara.{tabela}")
        .filter("operation = 'MERGE'")
        .select(F.lit(tabela).alias("tabela"), F.col("version").alias("versao"),
                F.date_format(F.from_utc_timestamp("timestamp", "America/Sao_Paulo"), "dd/MM HH:mm").alias("quando"),
                F.col("operationMetrics")["numTargetRowsInserted"].cast("bigint").alias("inseridas"),
                F.col("operationMetrics")["numTargetRowsUpdated"].cast("bigint").alias("atualizadas"))
    for tabela in TABELAS_PRATA
]
display(reduce(lambda a, b: a.unionByName(b), historicos).orderBy("tabela", "versao"))
