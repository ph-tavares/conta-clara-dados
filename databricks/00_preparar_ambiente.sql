-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Ambiente
-- MAGIC Esquema `conta_clara` e volumes `chegada` (arquivos do Airflow) e `entrega` (exportação para o MVP).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.conta_clara
COMMENT 'Conta Clara: consumo de energia, contas de luz e divergencias das familias do piloto';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.conta_clara.chegada
COMMENT 'Arquivos brutos coletados pelo Airflow (clima/, aneel/, leituras/)';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.conta_clara.entrega
COMMENT 'Arquivos exportados da Ouro para o MVP';

-- COMMAND ----------

SHOW VOLUMES IN workspace.conta_clara;
