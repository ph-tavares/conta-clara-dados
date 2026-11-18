-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Consultas
-- MAGIC
-- MAGIC | # | Pergunta |
-- MAGIC |---|---|
-- MAGIC | Q1 | Quais contas não batem com o medidor, e quanto foi cobrado a mais? |
-- MAGIC | Q2 | Quanto cada bairro gasta por dia e quanto deve dar a conta do mês com a tarifa e a bandeira vigentes? |
-- MAGIC | Q3 | O calor aumenta o consumo das famílias? |
-- MAGIC | Q4 | Quanto as contestações podem devolver às famílias, e onde está o problema: cobrança por média ou erro de leitura? (impacto) |
-- MAGIC | Q5 | Quais perfis de família (quem mora sozinho, quem tem Tarifa Social) a organização parceira deve acompanhar primeiro? |

-- COMMAND ----------

USE workspace.conta_clara;

-- COMMAND ----------

-- Q1: Quais contas não batem com o medidor, e quanto foi cobrado a mais?
CREATE OR REPLACE VIEW vw_q1_contas_divergentes AS
SELECT concat(left(fam.id_familia_hash, 10), '…')   AS familia,
       fam.bairro,
       d.data                                         AS leitura_da_distribuidora,
       f.tipo_faturamento,
       f.kwh_cobrado,
       f.kwh_esperado,
       round(100 * f.divergencia, 1)                  AS divergencia_pct,
       f.kwh_cobrados_a_mais,
       f.valor_a_mais_rs
FROM fato_conta f
JOIN dim_familia fam ON fam.sk_familia = f.sk_familia
JOIN dim_data d      ON d.sk_data = f.sk_data
WHERE f.fl_divergente = 1
ORDER BY f.valor_a_mais_rs DESC, divergencia_pct DESC;

SELECT * FROM vw_q1_contas_divergentes;

-- COMMAND ----------

-- Q2: Quanto cada bairro gasta por dia e quanto deve dar a conta do mês com a tarifa e a bandeira vigentes?
CREATE OR REPLACE VIEW vw_q2_consumo_e_conta_por_bairro AS
WITH por_familia AS (
  SELECT f.sk_familia, percentile_approx(f.consumo_diario_kwh, 0.5) AS consumo_diario
  FROM fato_leitura f
  WHERE f.consumo_diario_kwh IS NOT NULL AND f.fl_suspeita = 0
  GROUP BY f.sk_familia
),
vigente AS (
  SELECT d.data, d.bandeira, d.adicional_bandeira_rs_kwh
  FROM dim_data d WHERE d.data = (SELECT max(dd.data) FROM fato_leitura fl JOIN dim_data dd ON dd.sk_data = fl.sk_data)
)
SELECT fam.bairro,
       CASE WHEN fam.tarifa_social THEN 'Baixa Renda' ELSE 'Residencial' END AS tarifa,
       count(*)                                               AS familias,
       round(avg(p.consumo_diario), 2)                        AS kwh_por_dia,
       round(avg(p.consumo_diario) * 30, 0)                   AS kwh_em_30_dias,
       round(t.tarifa_rs_kwh, 4)                              AS tarifa_rs_kwh,
       v.bandeira,
       round(avg(p.consumo_diario) * 30 * (t.tarifa_rs_kwh + coalesce(v.adicional_bandeira_rs_kwh, 0)), 2) AS conta_30_dias_sem_impostos_rs
FROM por_familia p
JOIN dim_familia fam ON fam.sk_familia = p.sk_familia
CROSS JOIN vigente v
JOIN dim_tarifa t ON t.subclasse = CASE WHEN fam.tarifa_social THEN 'Baixa Renda' ELSE 'Residencial' END
                 AND v.data BETWEEN t.inicio_vigencia AND coalesce(t.fim_vigencia, DATE'9999-12-31')
GROUP BY fam.bairro, fam.tarifa_social, t.tarifa_rs_kwh, v.bandeira, v.adicional_bandeira_rs_kwh
ORDER BY fam.bairro, tarifa;

SELECT * FROM vw_q2_consumo_e_conta_por_bairro;

-- COMMAND ----------

-- Q3: O calor aumenta o consumo das famílias?
-- Compara o consumo de cada intervalo com o consumo típico da própria família, por faixa de temperatura máxima média do intervalo.
CREATE OR REPLACE VIEW vw_q3_calor_e_consumo AS
WITH tipico AS (
  SELECT sk_familia, percentile_approx(consumo_diario_kwh, 0.5) AS mediana
  FROM fato_leitura WHERE consumo_diario_kwh IS NOT NULL AND fl_suspeita = 0 GROUP BY sk_familia
)
SELECT CASE WHEN f.temp_max_media_periodo < 24 THEN '1. até 24 °C'
            WHEN f.temp_max_media_periodo < 28 THEN '2. 24 a 28 °C'
            WHEN f.temp_max_media_periodo < 32 THEN '3. 28 a 32 °C'
            ELSE '4. 32 °C ou mais' END                                   AS temperatura_maxima_no_periodo,
       count(*)                                                           AS intervalos,
       round(avg(f.temp_max_media_periodo), 1)                            AS temp_max_media_c,
       round(avg(f.consumo_diario_kwh), 2)                                AS kwh_por_dia,
       round(100 * (avg(f.consumo_diario_kwh / t.mediana) - 1), 1)        AS variacao_vs_tipico_da_familia_pct
FROM fato_leitura f
JOIN tipico t ON t.sk_familia = f.sk_familia
WHERE f.consumo_diario_kwh IS NOT NULL AND f.fl_suspeita = 0 AND f.temp_max_media_periodo IS NOT NULL
GROUP BY 1
ORDER BY 1;

SELECT * FROM vw_q3_calor_e_consumo;

-- COMMAND ----------

-- Q4 (impacto): Quanto as contestações podem devolver às famílias, e onde está o problema?
CREATE OR REPLACE VIEW vw_q4_impacto_contestacoes AS
SELECT coalesce(f.tipo_faturamento, 'total')                                  AS tipo_faturamento,
       sum(f.qtd_contas)                                                       AS contas_informadas,
       sum(f.fl_conferivel)                                                    AS contas_conferidas,
       sum(f.fl_divergente)                                                    AS contas_divergentes,
       round(100.0 * sum(f.fl_divergente) / nullif(sum(f.fl_conferivel), 0), 1) AS pct_divergentes,
       count(DISTINCT CASE WHEN f.valor_a_mais_rs > 0 THEN f.sk_familia END)   AS familias_com_contestacao,
       round(sum(f.kwh_cobrados_a_mais), 0)                                    AS kwh_cobrados_a_mais,
       round(sum(f.valor_a_mais_rs), 2)                                        AS valor_a_contestar_rs,
       round(sum(f.valor_a_mais_rs) / nullif(count(DISTINCT CASE WHEN f.valor_a_mais_rs > 0 THEN f.sk_familia END), 0), 2)
                                                                               AS valor_medio_por_familia_rs
FROM fato_conta f
GROUP BY ROLLUP (f.tipo_faturamento)
ORDER BY tipo_faturamento = 'total', tipo_faturamento;

SELECT * FROM vw_q4_impacto_contestacoes;

-- COMMAND ----------

-- Q5: Quais perfis de família a organização parceira deve acompanhar primeiro?
CREATE OR REPLACE VIEW vw_q5_perfis_para_acompanhamento AS
WITH ultima AS (
  SELECT sk_familia, max_by(fl_acima_do_habitual, sk_data) AS ultima_acima_do_habitual,
         percentile_approx(CASE WHEN fl_suspeita = 0 THEN consumo_diario_kwh END, 0.5) AS kwh_por_dia
  FROM fato_leitura GROUP BY sk_familia
),
contas AS (
  SELECT sk_familia, max(fl_divergente) AS tem_conta_divergente, sum(valor_a_mais_rs) AS valor_a_mais
  FROM fato_conta GROUP BY sk_familia
)
SELECT fam.faixa_moradores,
       CASE WHEN fam.tarifa_social THEN 'com Tarifa Social' ELSE 'sem Tarifa Social' END            AS tarifa_social,
       count(*)                                                                                   AS familias,
       round(avg(u.kwh_por_dia), 2)                                                               AS kwh_por_dia,
       sum(coalesce(u.ultima_acima_do_habitual, 0))                                               AS ultima_leitura_acima_do_habitual,
       sum(coalesce(c.tem_conta_divergente, 0))                                                   AS com_conta_divergente,
       round(sum(coalesce(c.valor_a_mais, 0)), 2)                                                 AS valor_a_contestar_rs,
       round(100.0 * sum(CASE WHEN coalesce(u.ultima_acima_do_habitual, 0) = 1 OR coalesce(c.tem_conta_divergente, 0) = 1
                              THEN 1 ELSE 0 END) / count(*), 1)                                    AS pct_para_acompanhar
FROM dim_familia fam
LEFT JOIN ultima u ON u.sk_familia = fam.sk_familia
LEFT JOIN contas c ON c.sk_familia = fam.sk_familia
GROUP BY fam.faixa_moradores, fam.tarifa_social
ORDER BY pct_para_acompanhar DESC, familias DESC;

SELECT * FROM vw_q5_perfis_para_acompanhamento;
