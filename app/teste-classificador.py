import requests
import pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score
from sklearn.preprocessing import MultiLabelBinarizer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime
import os
import sys
import time

# --- Configurações ---
CLASSIFICADOR_URL = os.getenv("CLASSIFICADOR_URL")
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
DATASET_FILE = os.getenv("DATASET_FILE")

print(f"Iniciando a leitura do dataset {DATASET_FILE}...")
df = pd.read_csv(DATASET_FILE)
df.dropna(subset=['texto', 'codigo_assunto'], inplace=True)
print(f"Dataset carregado com {len(df)} registros.")

# --- Listas para armazenar os resultados ---
resultados_detalhados = []
y_true_list = [] # Para métricas multi-rótulo
y_pred_list = [] # Para métricas multi-rótulo
acertos = 0
total_peticoes_processadas = 0

session = requests.Session()

# --- Loop Principal de Avaliação ---
for index, row in df.iterrows():
    print(f"Analisando a petição de número {index + 1}/{len(df)}...")
    # if index > 9:
    #     break
    texto = row["texto"]
    # Converte a string "123|456" para uma lista de inteiros [123, 456]
    cod_assuntos_esperados = [int(i.strip()) for i in str(row["codigo_assunto"]).split('|')]
    
    codigos_classificados = []
    
    try:
        response = session.post(CLASSIFICADOR_URL, json={"texto": texto}, timeout=20)
        response.raise_for_status()
        
        resultados_api = response.json().get("assuntos_selecionados", [])
        
        if not resultados_api:
            print("Atenção: A API retornou uma resposta vazia.")
        else:
            codigos_classificados = [res['codigo'] for res in resultados_api]

    except Exception as e:
        print(f"Erro ao processar a petição: {e}")
        # Mesmo com erro, continuamos para não quebrar o loop, mas a predição será vazia
    
    total_peticoes_processadas += 1

    # --- Lógica de Métrica Principal: "Acertou pelo menos um?" ---
    # Usamos conjuntos (sets) para encontrar a interseção de forma eficiente
    set_esperados = set(cod_assuntos_esperados)
    set_preditos = set(codigos_classificados)
    
    acertou_pelo_menos_um = len(set_esperados.intersection(set_preditos)) > 0
    
    if acertou_pelo_menos_um:
        acertos += 1

    # Armazena os resultados para as métricas multi-rótulo e para o CSV final
    y_true_list.append(cod_assuntos_esperados)
    y_pred_list.append(codigos_classificados)

    resultados_detalhados.append({
        "texto": texto,
        "cod_assuntos_esperados": '|'.join(map(str, cod_assuntos_esperados)),
        "codigos_preditos": ', '.join(map(str, codigos_classificados)),
        "acertou_pelo_menos_um": acertou_pelo_menos_um
    })
    
    time.sleep(2)

# --- Cálculo das Métricas ---
print("\nClassificação concluída.")
print("Iniciando cálculo das métricas...")

if total_peticoes_processadas == 0:
    print("Nenhuma petição foi processada. Encerrando.")
    sys.exit()

# Métrica 1: Taxa de Acerto
taxa_de_acerto = acertos / total_peticoes_processadas

# Métrica 2: Métricas Multi-Rótulo Completas (para uma visão mais profunda)
mlb = MultiLabelBinarizer()
all_labels = set(l for sublist in y_true_list for l in sublist).union(set(l for sublist in y_pred_list for l in sublist))
if not all_labels:
    print("Nenhum rótulo encontrado para calcular as métricas multi-rótulo.")
    f1_micro = precision_micro = recall_micro = 0.0
else:
    mlb.fit([list(all_labels)])
    y_true_bin = mlb.transform(y_true_list)
    y_pred_bin = mlb.transform(y_pred_list)
    
    precision_micro = precision_score(y_true_bin, y_pred_bin, average='micro', zero_division=0)
    recall_micro = recall_score(y_true_bin, y_pred_bin, average='micro', zero_division=0)
    f1_micro = f1_score(y_true_bin, y_pred_bin, average='micro', zero_division=0)

# --- Exibição e Salvamento ---
print("\n--- Métricas de Avaliação ---")
print(f"Acurácia: {taxa_de_acerto:.4f}")
print(f"Precisão: {precision_micro:.4f}")
print(f"Revocação: {recall_micro:.4f}")
print(f"F1-score: {f1_micro:.4f}")

print("\nSalvando resultados detalhados...")
pd.DataFrame(resultados_detalhados).to_csv("data/resultados_classificacao.csv", index=False)
print("Resultados salvos em data/resultados_classificacao.csv.")

# --- Envio para o InfluxDB ---
print("\nEnviando resultado para o Influxdb...")
try:
    metricas = [
        Point("classificacao_api")
            .tag("versao_modelo", "v1")
            .field("acuracia", taxa_de_acerto)
            .field("precisao", precision_micro)
            .field("revocacao", recall_micro)
            .field("f1_score", f1_micro)
            .time(datetime.datetime.now(), WritePrecision.NS)
    ]

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=metricas)
    client.close()
    print("Métricas enviadas para o InfluxDB com sucesso.")

except Exception as e:
    print(f"ERRO ao enviar dados para o InfluxDB: {e}")

print("\nScript finalizado.")