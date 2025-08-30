import os
import time
from influxdb_client import InfluxDBClient
from influxdb_client.rest import ApiException

print("--- Iniciando o script de diagnóstico ---")

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

print(f"URL: {INFLUX_URL}")
print(f"Token: {INFLUX_TOKEN}")
print(f"Org: {INFLUX_ORG}")
print(f"Bucket: {INFLUX_BUCKET}")

dado_teste = "classificacao_api,teste=valido valor=1.0"

client = None
try:
    print("\n[Passo 1] Tentando criar o cliente InfluxDB...")
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api()
    print("[Passo 1] Cliente criado com sucesso.")

    print("\n[Passo 2] Enviando dados...")
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=dado_teste)
    print("[Passo 2] Dados enviados para a fila com sucesso.")
    
    # Adiciona um pequeno delay para garantir que a fila termine
    # Esta linha é apenas para diagnóstico!
    time.sleep(2)
    
except ApiException as e:
    print("\n[ERRO API] Uma exceção de API ocorreu:")
    print(f"  Status: {e.status}")
    print(f"  Razão: {e.reason}")
    print(f"  Corpo do Erro: {e.body}")
except Exception as e:
    print("\n[ERRO GERAL] Ocorreu um erro inesperado:")
    print(e)
finally:
    print("\n[Passo 3] Entrando no bloco finally para fechar o cliente.")
    if client:
        client.close()
        print("[Passo 3] Conexão com o InfluxDB fechada com sucesso.")

print("--- Fim do script de diagnóstico ---")