import requests
import json
import pandas as pd
import time
import os
import threading
from shapely.geometry import shape, Point
from datetime import datetime
from fastapi import FastAPI
import shutil


# -------------------------------
# FASTAPI
# -------------------------------

app = FastAPI()
INTERVALO_SEGUNDOS = 300  # 5 minutos

# -------------------------------
# CONFIGURAÇÕES
# -------------------------------

API_URL = "http://transdaniel.m3.log.br/api/ultimaposicaoporoperacao"
API_KEY = "eVdLUnNOTXpQN1BxdlhzRTVPNnVQQWdxbTcwVjVRYlI3aTQ5OG1pbU9TZmZMQVhXT2xobDFvUXU1cmhO697ceaffa0583"

BASE_PATH = "/data"
os.makedirs(BASE_PATH, exist_ok=True)


POLYGON_FILE = f"{BASE_PATH}/UNIDADES.geojson"
PLANILHA_FILE = f"{BASE_PATH}/Base_Rastreio_Dedicados.xlsx"
ESTADO_FILE = f"{BASE_PATH}/estado_veiculos.json"

if not os.path.exists(POLYGON_FILE):
    shutil.copy("UNIDADES.geojson", POLYGON_FILE)
# -------------------------------
# FUNÇÕES
# -------------------------------

def carregar_poligonos(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        geojson = json.load(f)
    return {
        feature['properties']['id']: shape(feature['geometry'])
        for feature in geojson['features']
    }

def carregar_estado(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def salvar_estado(file_path, estado):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(estado, f)

def consultar_api():
    headers = {"x-api-key": API_KEY}
    resp = requests.get(API_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

def inicializar_planilha(planilha_file):
    if not os.path.exists(planilha_file):
        with pd.ExcelWriter(planilha_file) as writer:
            pd.DataFrame(
                columns=["veiculo","unidade","tipo","timestamp","lat","lon"]
            ).to_excel(writer, index=False, sheet_name="Eventos")
            pd.DataFrame(
                columns=["timestamp","status","mensagem"]
            ).to_excel(writer, index=False, sheet_name="Log_API")

def log_api(planilha_file, status, mensagem):
    now = datetime.now()
    try:
        log_df = pd.read_excel(planilha_file, sheet_name="Log_API")
    except:
        log_df = pd.DataFrame(columns=["timestamp","status","mensagem"])

    log_df = pd.concat([log_df, pd.DataFrame([{
        "timestamp": now,
        "status": status,
        "mensagem": mensagem
    }])], ignore_index=True)

    with pd.ExcelWriter(planilha_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        log_df.to_excel(writer, sheet_name="Log_API", index=False)

def processar_veiculos(dados, poligonos, estado, primeiro_run=False):
    eventos = []

    for v in dados:
        placa = v.get('placa')
        lat = v.get('latitude')
        lon = v.get('longitude')
        ts = v.get('dataposicao')

        if not (placa and lat and lon and ts):
            continue

        timestamp = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        ponto = Point(lon, lat)

        estado.setdefault(placa, {})

        for unidade, pol in poligonos.items():
            dentro = pol.contains(ponto)
            antes = estado[placa].get(unidade, False)

            if primeiro_run and dentro:
                eventos.append({
                    "veiculo": placa,
                    "unidade": unidade,
                    "tipo": "POSIÇÃO INICIAL",
                    "timestamp": timestamp,
                    "lat": lat,
                    "lon": lon
                })

            if not primeiro_run:
                if dentro and not antes:
                    eventos.append({
                        "veiculo": placa,
                        "unidade": unidade,
                        "tipo": "ENTRADA",
                        "timestamp": timestamp,
                        "lat": lat,
                        "lon": lon
                    })
                if not dentro and antes:
                    eventos.append({
                        "veiculo": placa,
                        "unidade": unidade,
                        "tipo": "SAÍDA",
                        "timestamp": timestamp,
                        "lat": lat,
                        "lon": lon
                    })

            estado[placa][unidade] = dentro

    return eventos

def salvar_planilha(eventos, planilha_file):
    if not eventos:
        return

    df_eventos = pd.DataFrame(eventos)
    try:
        df_existente = pd.read_excel(planilha_file, sheet_name="Eventos")
        df_final = pd.concat([df_existente, df_eventos], ignore_index=True)
    except:
        df_final = df_eventos

    with pd.ExcelWriter(planilha_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df_final.to_excel(writer, sheet_name="Eventos", index=False)

def executar_rastreio():
    inicializar_planilha(PLANILHA_FILE)
    poligonos = carregar_poligonos(POLYGON_FILE)

    primeiro_run = not os.path.exists(ESTADO_FILE)
    estado = carregar_estado(ESTADO_FILE)

    dados = consultar_api()
    eventos = processar_veiculos(dados, poligonos, estado, primeiro_run)

    salvar_planilha(eventos, PLANILHA_FILE)
    salvar_estado(ESTADO_FILE, estado)
    log_api(PLANILHA_FILE, "OK", f"{len(dados)} veículos, {len(eventos)} eventos")

    return len(eventos)

# -------------------------------
# BACKGROUND LOOP
# -------------------------------

def loop_background():
    while True:
        print(f"[{datetime.now()}] Executando rastreio...")
        try:
            executar_rastreio()
        except Exception as e:
            print("Erro:", e)
        time.sleep(INTERVALO_SEGUNDOS)

@app.on_event("startup")
def start_background():
    threading.Thread(target=loop_background, daemon=True).start()

@app.get("/health")
def health():
    return {"status": "ok"}
