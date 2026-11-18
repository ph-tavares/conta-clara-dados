# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Ouro
# MAGIC
# MAGIC | Tabela | Tipo | Grão |
# MAGIC |---|---|---|
# MAGIC | `fato_leitura` | fato | leitura válida do medidor, com o consumo desde a leitura anterior |
# MAGIC | `fato_conta` | fato | conta de luz, comparada com o consumo medido pelas leituras |
# MAGIC | `dim_familia` | dimensão | família do piloto (pseudonimizada) |
# MAGIC | `dim_data` | dimensão | dia, com temperatura e bandeira tarifária |
# MAGIC | `dim_tarifa` | dimensão | tarifa homologada B1 por subclasse e vigência |
# MAGIC | `ouro_painel_bairro` | agregada | situação de cada bairro (usada pelo MVP) |
# MAGIC
# MAGIC LGPD: a família vira hash SHA-256 com sal (informado no widget); nome e telefone não entram na Ouro;
# MAGIC o MVP recebe só agregados por bairro.

# COMMAND ----------

dbutils.widgets.text("salt_hash", "", "Sal do hash")
SAL = dbutils.widgets.get("salt_hash").strip()
assert len(SAL) >= 16, "Preencha o widget salt_hash (16 ou mais caracteres)."

spark.createDataFrame([(SAL,)], "sal STRING").createOrReplaceTempView("segredo_sal")
spark.sql("USE workspace.conta_clara")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensões

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_familia
# MAGIC COMMENT 'Familia do piloto, pseudonimizada: hash com sal no lugar do id, sem nome e sem telefone' AS
# MAGIC SELECT CAST(row_number() OVER (ORDER BY sha2(concat(s.sal, f.id_familia), 256)) AS INT) AS sk_familia,
# MAGIC        sha2(concat(s.sal, f.id_familia), 256) AS id_familia_hash,
# MAGIC        f.bairro,
# MAGIC        CASE WHEN f.moradores <= 1 THEN '1 pessoa' WHEN f.moradores <= 3 THEN '2 a 3 pessoas' ELSE '4 ou mais' END AS faixa_moradores,
# MAGIC        f.moradores <= 1 AS mora_sozinho,
# MAGIC        f.tarifa_social,
# MAGIC        date_format(f.cadastrada_em, 'yyyy-MM') AS mes_cadastro,
# MAGIC        f.origem
# MAGIC FROM prata_familia f CROSS JOIN segredo_sal s;
# MAGIC
# MAGIC -- ponte usada so durante a montagem dos fatos; nao fica gravada
# MAGIC CREATE OR REPLACE TEMP VIEW ponte_familia AS
# MAGIC SELECT f.id_familia, d.sk_familia, f.tarifa_social
# MAGIC FROM prata_familia f CROSS JOIN segredo_sal s
# MAGIC JOIN dim_familia d ON d.id_familia_hash = sha2(concat(s.sal, f.id_familia), 256);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_tarifa
# MAGIC COMMENT 'Tarifa B1 homologada pela ANEEL (TUSD + TE, sem impostos), por subclasse e vigencia' AS
# MAGIC SELECT CAST(row_number() OVER (ORDER BY subclasse, inicio_vigencia) AS INT) AS sk_tarifa,
# MAGIC        distribuidora, subclasse, inicio_vigencia, fim_vigencia, resolucao, tusd_rs_mwh, te_rs_mwh, tarifa_rs_kwh
# MAGIC FROM prata_tarifa
# MAGIC WHERE detalhe = 'Não se aplica' AND subclasse IN ('Residencial', 'Baixa Renda');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_data
# MAGIC COMMENT 'Calendario com temperatura (Open-Meteo) e bandeira tarifaria (ANEEL)' AS
# MAGIC WITH limites AS (
# MAGIC   SELECT least(min(data_leitura), (SELECT min(data) FROM prata_clima_hora)) AS inicio,
# MAGIC          greatest(max(data_leitura), (SELECT max(data_leitura_distribuidora) FROM prata_conta),
# MAGIC                   (SELECT max(data) FROM prata_clima_hora)) AS fim
# MAGIC   FROM prata_leitura
# MAGIC ),
# MAGIC dias AS (SELECT explode(sequence(inicio, fim, INTERVAL 1 DAY)) AS data FROM limites),
# MAGIC clima AS (
# MAGIC   SELECT data, round(max(temperatura_c), 1) AS temp_max_c, round(avg(temperatura_c), 1) AS temp_media_c,
# MAGIC          round(sum(precipitacao_mm), 1) AS chuva_mm
# MAGIC   FROM prata_clima_hora GROUP BY data
# MAGIC )
# MAGIC SELECT CAST(date_format(d.data, 'yyyyMMdd') AS INT) AS sk_data, d.data,
# MAGIC        element_at(array('domingo', 'segunda', 'terça', 'quarta', 'quinta', 'sexta', 'sábado'), dayofweek(d.data)) AS dia_semana,
# MAGIC        dayofweek(d.data) IN (1, 7) AS fim_de_semana,
# MAGIC        c.temp_max_c, c.temp_media_c, c.chuva_mm,
# MAGIC        CASE WHEN c.temp_max_c IS NULL THEN 'sem dado de clima'
# MAGIC             WHEN c.temp_max_c < 24 THEN '1. até 24 °C'
# MAGIC             WHEN c.temp_max_c < 28 THEN '2. 24 a 28 °C'
# MAGIC             WHEN c.temp_max_c < 32 THEN '3. 28 a 32 °C'
# MAGIC             ELSE '4. 32 °C ou mais' END AS faixa_temperatura,
# MAGIC        b.bandeira, round(b.adicional_rs_mwh / 1000, 5) AS adicional_bandeira_rs_kwh
# MAGIC FROM dias d
# MAGIC LEFT JOIN clima c ON c.data = d.data
# MAGIC LEFT JOIN prata_bandeira b ON b.competencia = trunc(d.data, 'MM');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fatos

# COMMAND ----------

# MAGIC %sql
# MAGIC -- consumo entre leituras consecutivas da mesma familia
# MAGIC CREATE OR REPLACE TEMP VIEW intervalos AS
# MAGIC SELECT l.*,
# MAGIC        lag(l.data_leitura) OVER (PARTITION BY l.id_familia ORDER BY l.data_leitura) AS data_anterior,
# MAGIC        lag(l.kwh_medidor)  OVER (PARTITION BY l.id_familia ORDER BY l.data_leitura) AS kwh_anterior
# MAGIC FROM prata_leitura l;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW intervalos_consumo AS
# MAGIC SELECT *, datediff(data_leitura, data_anterior) AS dias_desde_anterior,
# MAGIC        kwh_medidor - kwh_anterior AS consumo_kwh,
# MAGIC        round((kwh_medidor - kwh_anterior) / datediff(data_leitura, data_anterior), 3) AS consumo_diario_kwh
# MAGIC FROM intervalos;
# MAGIC
# MAGIC -- consumo tipico da familia: mediana dos intervalos (resiste a um digito errado que ainda nao foi corrigido)
# MAGIC CREATE OR REPLACE TEMP VIEW consumo_familia AS
# MAGIC SELECT id_familia, count(*) AS intervalos, percentile_approx(consumo_diario_kwh, 0.5) AS consumo_diario_mediano
# MAGIC FROM intervalos_consumo WHERE consumo_diario_kwh IS NOT NULL
# MAGIC GROUP BY id_familia;
# MAGIC
# MAGIC -- temperatura maxima media nos dias cobertos por cada intervalo
# MAGIC CREATE OR REPLACE TEMP VIEW temperatura_periodo AS
# MAGIC SELECT i.id_familia, i.data_leitura, round(avg(d.temp_max_c), 1) AS temp_max_media_periodo
# MAGIC FROM intervalos_consumo i
# MAGIC JOIN dim_data d ON d.data > i.data_anterior AND d.data <= i.data_leitura
# MAGIC GROUP BY i.id_familia, i.data_leitura;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE fato_leitura
# MAGIC COMMENT 'Fato: leitura valida do medidor e o consumo desde a leitura anterior da mesma familia' AS
# MAGIC SELECT p.sk_familia,
# MAGIC        CAST(date_format(i.data_leitura, 'yyyyMMdd') AS INT) AS sk_data,
# MAGIC        i.kwh_medidor, i.dias_desde_anterior, i.consumo_kwh, i.consumo_diario_kwh,
# MAGIC        tp.temp_max_media_periodo,
# MAGIC        i.origem_leitura,
# MAGIC        CAST(i.correcao AS INT) AS fl_corrigida,
# MAGIC        coalesce(CAST(i.consumo_diario_kwh < 0 OR i.consumo_diario_kwh > 3 * c.consumo_diario_mediano AS INT), 0) AS fl_suspeita,
# MAGIC        coalesce(CAST(i.consumo_diario_kwh > 1.3 * c.consumo_diario_mediano AND i.consumo_diario_kwh <= 3 * c.consumo_diario_mediano AS INT), 0) AS fl_acima_do_habitual,
# MAGIC        1 AS qtd_leituras
# MAGIC FROM intervalos_consumo i
# MAGIC JOIN ponte_familia p ON p.id_familia = i.id_familia
# MAGIC LEFT JOIN consumo_familia c ON c.id_familia = i.id_familia
# MAGIC LEFT JOIN temperatura_periodo tp ON tp.id_familia = i.id_familia AND tp.data_leitura = i.data_leitura;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fato_conta
# MAGIC COMMENT 'Fato: conta de luz comparada com o consumo medido pelas leituras da familia (limite de 15%, o mesmo do MVP)' AS
# MAGIC WITH base AS (
# MAGIC   SELECT c.*, p.sk_familia, p.tarifa_social, cf.consumo_diario_mediano, coalesce(cf.intervalos, 0) AS intervalos,
# MAGIC          t.sk_tarifa, t.tarifa_rs_kwh, d.adicional_bandeira_rs_kwh,
# MAGIC          round(cf.consumo_diario_mediano * c.dias_faturados, 1) AS kwh_esperado
# MAGIC   FROM prata_conta c
# MAGIC   JOIN ponte_familia p ON p.id_familia = c.id_familia
# MAGIC   LEFT JOIN consumo_familia cf ON cf.id_familia = c.id_familia
# MAGIC   LEFT JOIN dim_tarifa t ON t.subclasse = CASE WHEN p.tarifa_social THEN 'Baixa Renda' ELSE 'Residencial' END
# MAGIC                         AND c.data_leitura_distribuidora BETWEEN t.inicio_vigencia AND coalesce(t.fim_vigencia, DATE'9999-12-31')
# MAGIC   LEFT JOIN dim_data d ON d.data = c.data_leitura_distribuidora
# MAGIC )
# MAGIC SELECT sk_familia,
# MAGIC        CAST(date_format(data_leitura_distribuidora, 'yyyyMMdd') AS INT) AS sk_data,
# MAGIC        sk_tarifa,
# MAGIC        competencia, tipo_faturamento, dias_faturados, kwh_cobrado, valor_cobrado,
# MAGIC        consumo_diario_mediano AS consumo_diario_medido_kwh, intervalos AS intervalos_medidos,
# MAGIC        kwh_esperado,
# MAGIC        CASE WHEN intervalos >= 2 THEN round((kwh_cobrado - kwh_esperado) / kwh_esperado, 3) END AS divergencia,
# MAGIC        CAST(intervalos >= 2 AS INT) AS fl_conferivel,
# MAGIC        CAST(intervalos >= 2 AND abs(kwh_cobrado - kwh_esperado) / kwh_esperado > 0.15 AS INT) AS fl_divergente,
# MAGIC        CASE WHEN intervalos >= 2 AND (kwh_cobrado - kwh_esperado) / kwh_esperado > 0.15
# MAGIC             THEN round(kwh_cobrado - kwh_esperado, 1) ELSE 0 END AS kwh_cobrados_a_mais,
# MAGIC        CASE WHEN intervalos >= 2 AND (kwh_cobrado - kwh_esperado) / kwh_esperado > 0.15
# MAGIC             THEN round((kwh_cobrado - kwh_esperado) * (tarifa_rs_kwh + coalesce(adicional_bandeira_rs_kwh, 0)), 2) ELSE 0 END AS valor_a_mais_rs,
# MAGIC        1 AS qtd_contas
# MAGIC FROM base;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Painel do MVP: `ouro_painel_bairro`
# MAGIC Uma linha por bairro, só com totais e médias. A conta estimada usa a tarifa Residencial vigente e a bandeira do mês.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ouro_painel_bairro
# MAGIC COMMENT 'Situacao de cada bairro do piloto, usada pelo MVP em /api/dados/painel' AS
# MAGIC WITH referencia AS (
# MAGIC   SELECT max(d.data) AS data_referencia FROM fato_leitura f JOIN dim_data d ON d.sk_data = f.sk_data
# MAGIC ),
# MAGIC preco AS (
# MAGIC   SELECT t.tarifa_rs_kwh + coalesce(d.adicional_bandeira_rs_kwh, 0) AS rs_kwh
# MAGIC   FROM referencia r
# MAGIC   JOIN dim_data d ON d.data = r.data_referencia
# MAGIC   JOIN dim_tarifa t ON t.subclasse = 'Residencial' AND r.data_referencia BETWEEN t.inicio_vigencia AND coalesce(t.fim_vigencia, DATE'9999-12-31')
# MAGIC ),
# MAGIC por_familia AS (
# MAGIC   SELECT f.sk_familia,
# MAGIC          percentile_approx(CASE WHEN f.fl_suspeita = 0 THEN f.consumo_diario_kwh END, 0.5) AS consumo_diario,
# MAGIC          sum(f.qtd_leituras) AS leituras,
# MAGIC          max_by(f.fl_acima_do_habitual, f.sk_data) AS ultima_acima_do_habitual
# MAGIC   FROM fato_leitura f GROUP BY f.sk_familia
# MAGIC ),
# MAGIC contas AS (
# MAGIC   SELECT sk_familia, sum(qtd_contas) AS contas, sum(fl_conferivel) AS conferidas, sum(fl_divergente) AS divergentes,
# MAGIC          sum(CASE WHEN tipo_faturamento = 'media' THEN 1 ELSE 0 END) AS por_media,
# MAGIC          sum(kwh_cobrados_a_mais) AS kwh_a_mais, sum(valor_a_mais_rs) AS valor_a_mais
# MAGIC   FROM fato_conta GROUP BY sk_familia
# MAGIC )
# MAGIC SELECT d.bairro,
# MAGIC        count(*)                                                   AS familias,
# MAGIC        sum(CASE WHEN d.tarifa_social THEN 1 ELSE 0 END)           AS familias_tarifa_social,
# MAGIC        sum(coalesce(p.leituras, 0))                               AS leituras,
# MAGIC        round(avg(p.consumo_diario), 2)                            AS consumo_medio_diario_kwh,
# MAGIC        round(avg(p.consumo_diario) * 30 * (SELECT rs_kwh FROM preco), 2) AS conta_media_estimada_30d_rs,
# MAGIC        sum(coalesce(p.ultima_acima_do_habitual, 0))               AS familias_consumo_acima_do_habitual,
# MAGIC        sum(coalesce(c.contas, 0))                                 AS contas_informadas,
# MAGIC        sum(coalesce(c.conferidas, 0))                             AS contas_conferidas,
# MAGIC        sum(coalesce(c.divergentes, 0))                            AS contas_divergentes,
# MAGIC        sum(coalesce(c.por_media, 0))                              AS contas_por_media,
# MAGIC        round(sum(coalesce(c.kwh_a_mais, 0)), 1)                   AS kwh_cobrados_a_mais,
# MAGIC        round(sum(coalesce(c.valor_a_mais, 0)), 2)                 AS valor_a_contestar_rs,
# MAGIC        (SELECT data_referencia FROM referencia)                   AS data_referencia
# MAGIC FROM dim_familia d
# MAGIC LEFT JOIN por_familia p ON p.sk_familia = d.sk_familia
# MAGIC LEFT JOIN contas c      ON c.sk_familia = d.sk_familia
# MAGIC GROUP BY d.bairro;
# MAGIC
# MAGIC SELECT * FROM ouro_painel_bairro ORDER BY valor_a_contestar_rs DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência de LGPD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sk_familia, concat(left(id_familia_hash, 16), '…') AS id_familia_hash, bairro, faixa_moradores, tarifa_social, mes_cadastro, origem
# MAGIC FROM dim_familia ORDER BY sk_familia LIMIT 10;

# COMMAND ----------

proibidas = {"id_familia", "nome_responsavel", "telefone", "moradores"}
for tabela in ["dim_familia", "fato_leitura", "fato_conta", "ouro_painel_bairro"]:
    colunas = set(spark.table(f"workspace.conta_clara.{tabela}").columns)
    assert not (colunas & proibidas), f"Dado pessoal na Ouro ({tabela}): {colunas & proibidas}"
originais = spark.sql("""
  SELECT count(*) AS n FROM workspace.conta_clara.dim_familia d
  JOIN workspace.conta_clara.prata_familia f ON d.id_familia_hash = f.id_familia""").first()["n"]
assert originais == 0, "Há id de família sem hash na Ouro"
print("LGPD conferida: nenhuma tabela da Ouro tem id original, nome ou telefone.")
print("Colunas de dim_familia:", ", ".join(spark.table("workspace.conta_clara.dim_familia").columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência do modelo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fato_leitura' AS tabela, 'fato' AS papel, count(*) AS linhas FROM fato_leitura
# MAGIC UNION ALL SELECT 'fato_conta', 'fato', count(*) FROM fato_conta
# MAGIC UNION ALL SELECT 'dim_familia', 'dimensão', count(*) FROM dim_familia
# MAGIC UNION ALL SELECT 'dim_data', 'dimensão', count(*) FROM dim_data
# MAGIC UNION ALL SELECT 'dim_tarifa', 'dimensão', count(*) FROM dim_tarifa
# MAGIC UNION ALL SELECT 'ouro_painel_bairro', 'agregada para o MVP', count(*) FROM ouro_painel_bairro;
