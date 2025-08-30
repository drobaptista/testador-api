import requests
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.rest import ApiException
from influxdb_client .client.write_api import SYNCHRONOUS
import datetime
import os
import sys

CLASSIFICADOR_URL = os.getenv("CLASSIFICADOR_URL")
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
DATASET_FILE = os.getenv("DATASET_FILE")

df = pd.read_csv(DATASET_FILE)

resultados = []
y_true = []
y_pred = []

for _, row in df.iterrows():
    texto = row["texto"]
    cod_assunto_esperado = row["codigo_assunto"]
    # print(texto)
    # sys.exit()
    try:
        response = requests.post(CLASSIFICADOR_URL, json={"texto": texto}, timeout=10)
        response.raise_for_status()
        
        resultados_api = response.json()
        # print(resultados_api)
        # sys.exit()
        if not resultados_api:
            print(f"Atenção: A API retornou uma resposta vazia para o texto: {texto}")
            continue
        
        codigos_classificados = [res['codigo'] for res in resultados_api]
        
        acertou = cod_assunto_esperado in codigos_classificados
        
        if acertou:
            predicao_final = cod_assunto_esperado
        else:
            predicao_final = codigos_classificados[0] if codigos_classificados else "NENHUM"

    except requests.exceptions.HTTPError as err:
        print(f"Erro ao processar a petição. Erro: {err}")
        print("---------------------------------")
        print("Detalhes do erro da API:")
        print(response.text)


    except Exception as e:
        print(f"Erro ao processar a petição. Erro: {e}")
        predicao_final = "ERRO"
        
    resultados.append({
        "texto": texto[:50] + "...",
        "cod_assunto_esperado": cod_assunto_esperado,
        "codigos_preditos": ', '.join(map(str, codigos_classificados)) if 'codigos_classificados' in locals() else 'ERRO',
        "predicao_para_metrica": predicao_final
    })

    y_true.append(cod_assunto_esperado)
    y_pred.append(predicao_final)

if not y_true or not y_pred:
    print("A lista está vazia. O script será encerrado.")
    sys.exit()

acuracia = accuracy_score(y_true, y_pred)
precisao = precision_score(y_true, y_pred, average="weighted", zero_division=0)
revocacao = recall_score(y_true, y_pred, average="weighted", zero_division=0)
f1 = f1_score(y_true, y_pred, average="weighted", zero_division=0)

print(f"\nAcurácia: {acuracia:.4f}")
print(f"Precisão: {precisao:.4f}")
print(f"Revocação: {revocacao:.4f}")
print(f"F1-score: {f1:.4f}")

pd.DataFrame(resultados).to_csv("data/resultados_classificacao.csv", index=False)
print("Resultados salvos em resultados_classificacao.csv")

metricas = [
    Point("classificacao_api")
        .tag("versao_modelo", "v1")
        .field("acuracia", acuracia)
        .field("precisao", precisao)
        .field("revocacao", revocacao)
        .field("f1_score", f1)
        .time(datetime.datetime.now(), WritePrecision.NS)
]

try:
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=metricas)
    client.close()
    print("Métricas enviadas para o InfluxDB com sucesso!")
except ApiException as e:
    print("ERRO ao enviar dados para o InfluxDB:")
    print(f"  Status: {e.status}")
    print(f"  Razão: {e.reason}")
    print(f"  Corpo do Erro: {e.body}")
    print("Verifique seu token, organização e bucket.")
except Exception as e:
    print("Ocorreu um erro inesperado:")
    print(e)