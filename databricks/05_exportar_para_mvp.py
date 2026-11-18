# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Exportação para o MVP
# MAGIC Grava no Volume `entrega`:
# MAGIC - `ouro_painel_bairro.json`: tabela `ouro_painel_bairro` no formato de `contrato/ouro_painel_bairro.schema.json`, servida pelo MVP em `/api/dados/painel`;
# MAGIC - `resumo_execucao.json`: contagens e resultados das consultas.

# COMMAND ----------

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

ESQUEMA = "workspace.conta_clara"
SAIDA = "/Volumes/workspace/conta_clara/entrega"
os.makedirs(SAIDA, exist_ok=True)


def linhas(sql: str) -> list[dict]:
    return [{coluna: float(valor) if isinstance(valor, Decimal) else valor
             for coluna, valor in linha.asDict(recursive=True).items()} for linha in spark.sql(sql).collect()]


def gravar(nome: str, conteudo: dict) -> None:
    with open(f"{SAIDA}/{nome}", "w", encoding="utf-8") as arquivo:
        json.dump(conteudo, arquivo, ensure_ascii=False, indent=1, default=str)
    print(f"Gravado: {SAIDA}/{nome}")


agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Painel do MVP

# COMMAND ----------

bairros = linhas(f"""
  SELECT bairro, familias, familias_tarifa_social, leituras, consumo_medio_diario_kwh, conta_media_estimada_30d_rs,
         familias_consumo_acima_do_habitual, contas_informadas, contas_conferidas, contas_divergentes, contas_por_media,
         kwh_cobrados_a_mais, valor_a_contestar_rs
  FROM {ESQUEMA}.ouro_painel_bairro ORDER BY bairro""")
referencia = linhas(f"SELECT CAST(max(data_referencia) AS STRING) AS d FROM {ESQUEMA}.ouro_painel_bairro")[0]["d"]
tarifa = linhas(f"""
  SELECT t.distribuidora, t.resolucao, CAST(t.inicio_vigencia AS STRING) AS inicio_vigencia,
         max(CASE WHEN t.subclasse = 'Residencial' THEN t.tarifa_rs_kwh END) AS tarifa_residencial_rs_kwh,
         max(CASE WHEN t.subclasse = 'Baixa Renda' THEN t.tarifa_rs_kwh END) AS tarifa_baixa_renda_rs_kwh
  FROM {ESQUEMA}.dim_tarifa t
  WHERE DATE'{referencia}' BETWEEN t.inicio_vigencia AND coalesce(t.fim_vigencia, DATE'9999-12-31')
  GROUP BY ALL""")[0]
bandeira = linhas(f"""
  SELECT bandeira, adicional_bandeira_rs_kwh, temp_max_c
  FROM {ESQUEMA}.dim_data WHERE data = DATE'{referencia}'""")[0]

painel = {
    "tabela_origem": f"{ESQUEMA}.ouro_painel_bairro",
    "gerado_em": agora,
    "data_referencia": referencia,
    "demonstracao": True,
    "referencia_tarifaria": {**tarifa, "bandeira": bandeira["bandeira"],
                             "adicional_bandeira_rs_kwh": bandeira["adicional_bandeira_rs_kwh"]},
    "fontes": ["Leituras e contas das famílias (demonstração)", "ANEEL dados abertos", "Open-Meteo (CC BY 4.0)"],
    "bairros": bairros,
}
gravar("ouro_painel_bairro.json", painel)
print(f"{len(bairros)} bairros publicados, dados até {referencia}.")
display(spark.table(f"{ESQUEMA}.ouro_painel_bairro").orderBy("bairro"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resumo da execução

# COMMAND ----------

TABELAS = ["bronze_clima", "bronze_aneel", "bronze_leituras",
           "prata_familia", "prata_leitura", "prata_conta", "prata_clima_hora", "prata_tarifa",
           "prata_bandeira", "prata_rejeitados",
           "fato_leitura", "fato_conta", "dim_familia", "dim_data", "dim_tarifa", "ouro_painel_bairro"]
CONSULTAS = {
    "q1": "vw_q1_contas_divergentes",
    "q2": "vw_q2_consumo_e_conta_por_bairro",
    "q3": "vw_q3_calor_e_consumo",
    "q4": "vw_q4_impacto_contestacoes",
    "q5": "vw_q5_perfis_para_acompanhamento",
}


def merges(tabela: str) -> list[dict]:
    return [{"tabela": tabela, "versao": h["version"], "quando": str(h["timestamp"]),
             "inseridas": int(h["operationMetrics"].get("numTargetRowsInserted", 0)),
             "atualizadas": int(h["operationMetrics"].get("numTargetRowsUpdated", 0))}
            for h in spark.sql(f"DESCRIBE HISTORY {ESQUEMA}.{tabela}").filter("operation = 'MERGE'")
                          .orderBy("version").collect()]


resumo = {
    "gerado_em": agora,
    "contagens": {t: spark.table(f"{ESQUEMA}.{t}").count() for t in TABELAS},
    "bronze_controle": linhas(f"""
        SELECT dense_rank() OVER (ORDER BY executado_em) AS execucao,
               date_format(from_utc_timestamp(executado_em, 'America/Sao_Paulo'), 'dd/MM/yyyy HH:mm:ss') AS quando,
               tabela, linhas_antes, linhas_depois, arquivos_novos
        FROM {ESQUEMA}.bronze_controle_cargas ORDER BY executado_em, tabela"""),
    "bronze_arquivos": linhas(f"""
        SELECT 'clima' AS fonte, CAST(data_referencia AS STRING) AS dia, count(*) AS vezes FROM {ESQUEMA}.bronze_clima GROUP BY ALL
        UNION ALL SELECT 'aneel', CAST(data_referencia AS STRING), count(*) FROM {ESQUEMA}.bronze_aneel GROUP BY ALL
        UNION ALL SELECT 'leituras', CAST(data_referencia AS STRING), count(*) FROM {ESQUEMA}.bronze_leituras GROUP BY ALL
        ORDER BY fonte, dia"""),
    "qualidade": linhas(f"""
        SELECT fonte, regra, descricao, total_avaliado, total_barrado FROM {ESQUEMA}.prata_qualidade
        WHERE executado_em = (SELECT max(executado_em) FROM {ESQUEMA}.prata_qualidade) ORDER BY regra, fonte"""),
    "qualidade_historico": linhas(f"""
        SELECT dense_rank() OVER (ORDER BY executado_em) AS execucao, fonte, regra, total_avaliado, total_barrado
        FROM {ESQUEMA}.prata_qualidade ORDER BY executado_em, regra, fonte"""),
    "merges": [m for t in ["prata_familia", "prata_leitura", "prata_conta", "prata_clima_hora",
                           "prata_tarifa", "prata_bandeira"] for m in merges(t)],
    "consultas": {q: linhas(f"SELECT * FROM {ESQUEMA}.{view}") for q, view in CONSULTAS.items()},
    "painel": painel,
    "lgpd": {
        "colunas_dim_familia": spark.table(f"{ESQUEMA}.dim_familia").columns,
        "colunas_fato_conta": spark.table(f"{ESQUEMA}.fato_conta").columns,
        "exemplo_hash": linhas(f"SELECT concat(left(id_familia_hash, 16), '…') AS h FROM {ESQUEMA}.dim_familia LIMIT 1"),
    },
}
gravar("resumo_execucao.json", resumo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contagens

# COMMAND ----------

display(spark.createDataFrame([(t, n) for t, n in resumo["contagens"].items()], "tabela STRING, linhas BIGINT"))
