from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
import polars as pl
import os
from dotenv import load_dotenv

# Carrega .env
load_dotenv()

# Caminho e credenciais
KEY_PATH = "itvalley-6eeeaaa638ec.json"
PROPERTY_ID = os.getenv("PROPERTY_ID")

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = BetaAnalyticsDataClient(credentials=credentials)

# Requisição: campanha + origem + canal + data
request = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    dimensions=[
        Dimension(name="date"),
        Dimension(name="sessionCampaignName"),
        Dimension(name="sessionSource"),
        Dimension(name="sessionDefaultChannelGroup")
    ],
    metrics=[Metric(name="sessions")],
    date_ranges=[DateRange(start_date="2025-05-01", end_date="yesterday")]
)

response = client.run_report(request)

# Processa os dados
dados = []
for row in response.rows:
    dados.append({
        "Data": row.dimension_values[0].value,
        "Campanha": row.dimension_values[1].value,
        "Origem": row.dimension_values[2].value,
        "Canal": row.dimension_values[3].value,
        "Sessões": int(row.metric_values[0].value)
    })

# Cria DataFrame com Polars
df = pl.DataFrame(dados)

# Exibe no terminal
print("✅ Sessões por campanha, origem e canal:")
print(df)

# (Descomente para salvar)
# df.write_csv("sessoes_campanha_origem_canal.csv")
# df.write_ndjson("sessoes_campanha_origem_canal.json")
