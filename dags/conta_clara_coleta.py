from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pendulum
import requests
from airflow.sdk import Variable, dag, task

sys.path.insert(0, str(Path(__file__).parent))
from simulador_medidores import gerar_dia  # noqa: E402

FUSO = "America/Sao_Paulo"
CHEGADA = Path(os.getenv("CHEGADA_DIR", "/opt/airflow/chegada"))
AMOSTRAS = Path(os.getenv("AMOSTRAS_DIR", "/opt/airflow/amostras"))
AGENTE = {"User-Agent": "conta-clara/1.0 (FIAP Global Solution)"}

# Zona sul de Sao Paulo (Capao Redondo / Jardim Angela), area da Enel SP (ELETROPAULO)
PILOTO = (-23.66, -46.76)
CNPJ_DISTRIBUIDORA = "61695227000193"
OPEN_METEO_PADRAO = "https://api.open-meteo.com/v1/forecast"
ANEEL_PADRAO = "https://dadosabertos.aneel.gov.br/api/3/action"
ANEEL_TARIFAS = "fcf2906c-7c32-4b9b-a637-054e7a5234f4"
ANEEL_ACIONAMENTO = "0591b8f6-fe54-437b-b72b-1aa2efd46e42"
ANEEL_ADICIONAL = "5879ca80-b3bd-45b1-a135-d9b77c1d5b36"

# fonte -> bloco que precisa trazer registros
CAMPO_REGISTROS = {"clima": "hourly", "aneel": "tarifas", "leituras": "leituras"}


def dia_referencia(contexto: dict) -> str:
    base = contexto.get("logical_date") or contexto["dag_run"].run_after
    return pendulum.instance(base).in_timezone(FUSO).subtract(days=1).to_date_string()


def caminho(fonte: str, dia: str) -> Path:
    return CHEGADA / fonte / f"{fonte}_{dia}.json"


def gravar(fonte: str, dia: str, conteudo: dict) -> str:
    destino = caminho(fonte, dia)
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(".tmp")
    temporario.write_text(json.dumps(conteudo, ensure_ascii=False, indent=1), encoding="utf-8")
    temporario.replace(destino)
    return str(destino)


def ckan(base: str, acao: str, **params) -> dict:
    resposta = requests.get(f"{base}/{acao}", params=params, headers=AGENTE, timeout=60)
    resposta.raise_for_status()
    corpo = resposta.json()
    if not corpo.get("success"):
        raise ValueError(f"{acao} falhou: {corpo.get('error')}")
    return corpo["result"]


def notificar(titulo: str, mensagem: str, etiqueta: str) -> None:
    url = Variable.get("ntfy_topic_secret", default=None)
    if not url:
        print(f"[sem ntfy_topic_secret] {titulo}: {mensagem}")
        return
    requests.post(url, data=mensagem.encode("utf-8"), timeout=10,
                  headers={"Title": titulo, "Tags": etiqueta}).raise_for_status()


def avisar_falha(contexto: dict) -> None:
    ti = contexto["task_instance"]
    notificar("Conta Clara: coleta FALHOU",
              f"DAG {ti.dag_id}, tarefa {ti.task_id}, execucao {contexto.get('run_id')}", "rotating_light")


@dag(
    dag_id="conta_clara_coleta",
    description="Coleta diaria: clima (Open-Meteo), tarifas e bandeiras (ANEEL) e leituras -> chegada/",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 3, tz=FUSO),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=1),
        "retry_exponential_backoff": True,
        "on_failure_callback": avisar_falha,
    },
    tags=["conta-clara", "global-solution", "coleta"],
)
def conta_clara_coleta():

    @task
    def coletar_clima(**contexto) -> str:
        dia = dia_referencia(contexto)
        resposta = requests.get(Variable.get("open_meteo_url", default=OPEN_METEO_PADRAO), headers=AGENTE, timeout=30,
                                params={"latitude": PILOTO[0], "longitude": PILOTO[1], "start_date": dia, "end_date": dia,
                                        "hourly": "temperature_2m,apparent_temperature,precipitation", "timezone": FUSO})
        resposta.raise_for_status()
        return gravar("clima", dia, resposta.json())

    @task
    def coletar_aneel(**contexto) -> str:
        dia = dia_referencia(contexto)
        base = Variable.get("aneel_api_url", default=ANEEL_PADRAO)
        filtros = {"NumCNPJDistribuidora": CNPJ_DISTRIBUIDORA, "DscSubGrupo": "B1",
                   "DscBaseTarifaria": "Tarifa de Aplicação", "DscModalidadeTarifaria": "Convencional"}
        tarifas = ckan(base, "datastore_search", resource_id=ANEEL_TARIFAS, filters=json.dumps(filtros),
                       sort="DatInicioVigencia desc", limit=500)["records"]
        vigentes = [t for t in tarifas if t["DatInicioVigencia"] <= dia <= (t["DatFimVigencia"] or "9999-12-31")]
        competencia = dia[:8] + "01"
        acionamento = ckan(base, "datastore_search", resource_id=ANEEL_ACIONAMENTO,
                           filters=json.dumps({"DatCompetencia": competencia}), limit=10)["records"]
        adicional = ckan(base, "datastore_search", resource_id=ANEEL_ADICIONAL, limit=100)["records"]
        adicional_vigente = max((a["DatVigencia"] for a in adicional if a["DatVigencia"] <= dia), default=None)
        return gravar("aneel", dia, {
            "dia_referencia": dia, "distribuidora": "ELETROPAULO (Enel SP)", "cnpj": CNPJ_DISTRIBUIDORA,
            "tarifas": vigentes, "bandeira_acionamento": acionamento,
            "bandeira_adicional": [a for a in adicional if a["DatVigencia"] == adicional_vigente]})

    @task
    def gerar_leituras(**contexto) -> str:
        dia = dia_referencia(contexto)
        return gravar("leituras", dia, gerar_dia(date.fromisoformat(dia), CHEGADA))

    @task
    def conferir_chegada(**contexto) -> dict:
        dia = dia_referencia(contexto)
        resumo, problemas = {}, []
        for fonte, campo in CAMPO_REGISTROS.items():
            arquivo = caminho(fonte, dia)
            if not arquivo.is_file() or arquivo.stat().st_size == 0:
                problemas.append(f"{arquivo.name}: nao chegou ou esta vazio")
                continue
            bloco = json.loads(arquivo.read_text(encoding="utf-8")).get(campo)
            total = len(bloco["time"]) if fonte == "clima" and bloco else len(bloco or [])
            if total == 0:
                problemas.append(f"{arquivo.name}: sem registros em '{campo}'")
                continue
            resumo[fonte] = {"arquivo": arquivo.name, "bytes": arquivo.stat().st_size, "registros": total}
            print(f"OK {arquivo.name}: {arquivo.stat().st_size} bytes, {total} registros")
        if problemas:
            raise ValueError("Conferencia da chegada falhou: " + "; ".join(problemas))
        return {"dia": dia, "fontes": resumo}

    @task
    def atualizar_amostras(conferencia: dict) -> None:
        AMOSTRAS.mkdir(parents=True, exist_ok=True)
        for fonte, campo in CAMPO_REGISTROS.items():
            dados = json.loads(caminho(fonte, conferencia["dia"]).read_text(encoding="utf-8"))
            if fonte == "clima":
                dados[campo] = {chave: valores[:5] for chave, valores in dados[campo].items()}
            else:
                dados[campo] = dados[campo][:5]
            if fonte == "leituras":
                dados["familias"] = [{**f, "nome_responsavel": "***", "telefone": "***"} for f in dados["familias"][:3]]
                dados["contas"] = dados["contas"][:3]
            (AMOSTRAS / f"{fonte}_amostra.json").write_text(
                json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")

    @task
    def notificar_sucesso(conferencia: dict) -> None:
        linhas = [f"{f}: {d['registros']} registros ({d['bytes']} bytes)" for f, d in conferencia["fontes"].items()]
        notificar(f"Conta Clara: coleta de {conferencia['dia']} ok", "\n".join(linhas), "white_check_mark")

    clima = coletar_clima()
    leituras = gerar_leituras()
    clima >> leituras
    conferencia = conferir_chegada()
    [clima, coletar_aneel(), leituras] >> conferencia
    atualizar_amostras(conferencia)
    notificar_sucesso(conferencia)


conta_clara_coleta()
