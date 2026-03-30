from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
import polars as pl
import os
from dotenv import load_dotenv

# Carrega as variáveis do .env
load_dotenv()

# Caminho para o JSON e ID da propriedade
KEY_PATH = "itvalley-6eeeaaa638ec.json"  # ou use os.getenv("KEY_PATH")
PROPERTY_ID = os.getenv("PROPERTY_ID")

# Autenticação
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = BetaAnalyticsDataClient(credentials=credentials)

# Requisição de eventos por data
request = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    dimensions=[
        Dimension(name="date"),
        Dimension(name="eventName")
    ],
    metrics=[Metric(name="eventCount")],
    date_ranges=[DateRange(start_date="2025-05-01", end_date="yesterday")]
)

response = client.run_report(request)

# Montagem dos dados em lista de dicionários
dados = []
for row in response.rows:
    dados.append({
        "Data": row.dimension_values[0].value,
        "Evento": row.dimension_values[1].value,
        "Total": int(row.metric_values[0].value)
    })

# Criação do DataFrame Polars
df = pl.DataFrame(dados)

# Exibe resultado no terminal
print(df)

# (Opcional) Salvar como CSV e JSON
# df.write_csv("eventos_por_data.csv")
# df.write_ndjson("eventos_por_data.json")





