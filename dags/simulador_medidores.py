"""Leituras de medidor e contas de luz SINTETICAS das familias de demonstracao do piloto.

Cada dia de referencia gera sempre o mesmo arquivo (semente = familia + data). O consumo diario usa a
temperatura maxima real do dia (arquivo do Open-Meteo ja coletado), para simular o uso de ventilador e
chuveiro em dias quentes. Erros de proposito para a Prata barrar: leitura duplicada, valor negativo,
familia inexistente, foto com baixa confianca, conta invalida e um digito errado corrigido no dia seguinte.
"""
from __future__ import annotations

import json
import random
from datetime import date, timedelta
from pathlib import Path

INICIO_PILOTO = date(2026, 11, 2)
INICIO_CONSUMO = date(2026, 9, 1)
FAMILIAS_INICIAIS = 48
NOVA_FAMILIA_A_CADA = 3
BAIRROS = ["Capão Redondo", "Jardim Ângela", "Grajaú", "Campo Limpo"]


def _rng(*partes) -> random.Random:
    return random.Random("|".join(str(p) for p in partes))


def familia(n: int) -> dict:
    r = _rng("familia", n)
    moradores = r.choices([1, 2, 3, 4, 5, 6], weights=[18, 22, 24, 18, 11, 7])[0]
    cadastro = INICIO_PILOTO if n <= FAMILIAS_INICIAIS else \
        INICIO_PILOTO + timedelta(days=(n - FAMILIAS_INICIAIS) * NOVA_FAMILIA_A_CADA)
    return {
        "id_familia": f"demo-{n:03d}",
        "nome_responsavel": f"Responsavel Demonstracao {n:03d}",
        "telefone": f"(11) 90000-{n:04d}",
        "bairro": r.choice(BAIRROS),
        "moradores": moradores,
        "tarifa_social": r.random() < 0.55,
        "cadastrada_em": cadastro.isoformat(),
        "_base_kwh_dia": round(1.6 + 1.25 * moradores + r.uniform(-0.6, 1.4), 2),
        "_ciclo": r.randint(1, 28),
        "_cobranca_por_media": r.random() < 0.25,
        "_erro_leitura": r.random() < 0.08,
        "_medidor_inicial": r.randint(8000, 60000),
    }


def familias_ate(dia: date) -> list[dict]:
    ultimas = FAMILIAS_INICIAIS + max(0, (dia - INICIO_PILOTO).days) // NOVA_FAMILIA_A_CADA
    return [f for f in (familia(n) for n in range(1, ultimas + 1)) if date.fromisoformat(f["cadastrada_em"]) <= dia]


def _temperatura_maxima(chegada: Path, dia: date) -> float | None:
    arquivo = chegada / "clima" / f"clima_{dia.isoformat()}.json"
    if not arquivo.is_file():
        return None
    temperaturas = [t for t in json.loads(arquivo.read_text(encoding="utf-8"))["hourly"]["temperature_2m"] if t is not None]
    return max(temperaturas) if temperaturas else None


def consumo_do_dia(f: dict, dia: date, tmax: float | None) -> float:
    r = _rng("consumo", f["id_familia"], dia)
    calor = 1 + 0.06 * max(0.0, (tmax or 27.0) - 27.0)
    fim_de_semana = 1.12 if dia.weekday() >= 5 else 1.0
    return f["_base_kwh_dia"] * r.uniform(0.82, 1.18) * calor * fim_de_semana


def medidor(f: dict, dia: date, temperaturas: dict[date, float | None]) -> int:
    total = sum(consumo_do_dia(f, INICIO_CONSUMO + timedelta(days=i), temperaturas.get(INICIO_CONSUMO + timedelta(days=i)))
                for i in range((dia - INICIO_CONSUMO).days + 1))
    return int(f["_medidor_inicial"] + total)


def _publico(f: dict) -> dict:
    return {k: v for k, v in f.items() if not k.startswith("_")}


def _leitura(f: dict, dia: date, kwh: int, correcao: bool = False) -> dict:
    r = _rng("foto", f["id_familia"], dia)
    por_foto = r.random() < 0.8
    confianca = round(r.uniform(0.58, 0.99), 2) if por_foto else None
    return {
        "id_familia": f["id_familia"], "data_leitura": dia.isoformat(), "kwh_medidor": kwh,
        "origem_leitura": "foto" if por_foto else "digitada", "confianca": confianca,
        "confirmada_pelo_cidadao": (not por_foto) or confianca >= 0.7 or r.random() < 0.5,
        "correcao": correcao,
    }


def gerar_dia(dia: date, chegada: Path) -> dict:
    temperaturas = {INICIO_CONSUMO + timedelta(days=i): _temperatura_maxima(chegada, INICIO_CONSUMO + timedelta(days=i))
                    for i in range((dia - INICIO_CONSUMO).days + 1)}
    cadastradas = familias_ate(dia)
    novas = [f for f in cadastradas if f["cadastrada_em"] == dia.isoformat()]
    erro = _rng("erros", dia)

    familias = [_publico(f) for f in novas]
    if dia == INICIO_PILOTO + timedelta(days=8):
        # recadastro: duas familias passam a ter Tarifa Social depois da orientacao no CRAS
        for f in [f for f in cadastradas if not f["tarifa_social"] and f not in novas][:2]:
            familias.append({**_publico(f), "tarifa_social": True})

    leituras = []
    for f in cadastradas:
        if f in novas or _rng("envia", f["id_familia"], dia).random() < 0.35:
            leituras.append(_leitura(f, dia, medidor(f, dia, temperaturas)))

    if leituras and dia > INICIO_PILOTO:
        typo = erro.choice(leituras)
        if not typo["correcao"]:
            typo["kwh_medidor"] += 100  # digito trocado na foto; a correcao chega no arquivo do dia seguinte
    ontem = dia - timedelta(days=1)
    if ontem > INICIO_PILOTO:
        anterior = [f for f in familias_ate(ontem)
                    if f["cadastrada_em"] == ontem.isoformat() or _rng("envia", f["id_familia"], ontem).random() < 0.35]
        if anterior:
            corrigida = _rng("erros", ontem).choice(anterior)
            leituras.append(_leitura(corrigida, ontem, medidor(corrigida, ontem, temperaturas), correcao=True))

    if leituras and erro.random() < 0.5:
        leituras.append(dict(erro.choice(leituras)))
    if erro.random() < 0.35:
        leituras.append({**_leitura(erro.choice(cadastradas), dia, 0), "kwh_medidor": -erro.randint(1000, 50000)})
    if erro.random() < 0.35:
        leituras.append({**_leitura(familia(999), dia, erro.randint(1000, 50000)), "id_familia": "demo-999"})

    contas = []
    for f in cadastradas:
        if f["_ciclo"] != dia.day:
            continue
        r = _rng("conta", f["id_familia"], dia)
        dias = r.randint(29, 32)
        real = medidor(f, dia, temperaturas) - medidor(f, dia - timedelta(days=dias), temperaturas)
        if f["_cobranca_por_media"]:
            tipo, cobrado = "media", round(f["_base_kwh_dia"] * dias * r.uniform(1.25, 1.7))
        elif f["_erro_leitura"]:
            tipo, cobrado = "leitura", round(real * r.uniform(1.3, 1.5))
        else:
            tipo, cobrado = "leitura", round(real * r.uniform(0.98, 1.03))
        contas.append({
            "id_conta": f"{f['id_familia']}-{dia:%Y%m}", "id_familia": f["id_familia"],
            "competencia": f"{dia:%Y-%m}", "data_leitura_distribuidora": dia.isoformat(),
            "dias_faturados": dias, "kwh_cobrado": cobrado, "valor_cobrado": round(cobrado * 0.93, 2),
            "tipo_faturamento": tipo,
        })
    if contas and erro.random() < 0.3:
        contas.append({**contas[0], "id_conta": contas[0]["id_conta"] + "-x", "dias_faturados": 0, "kwh_cobrado": None})

    return {
        "dia_referencia": dia.isoformat(),
        "origem": "sintetico",
        "aviso": "Dados de DEMONSTRACAO: familias, leituras e contas ficticias",
        "familias": familias,
        "leituras": leituras,
        "contas": contas,
    }
