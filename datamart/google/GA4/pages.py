from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
import polars as pl
import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

# Caminho para o JSON e ID da propriedade
KEY_PATH = "itvalley-6eeeaaa638ec.json"  # Ou os.getenv("KEY_PATH")
PROPERTY_ID = os.getenv("PROPERTY_ID")

# Autenticação
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = BetaAnalyticsDataClient(credentials=credentials)

# Requisição: visualizações por título da página e por data
request = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    dimensions=[
        Dimension(name="date"),
        Dimension(name="pageTitle")
    ],
    metrics=[Metric(name="screenPageViews")],
    date_ranges=[DateRange(start_date="2025-05-01", end_date="yesterday")]
)

# Executa a consulta
response = client.run_report(request)

# Processa os dados
dados_paginas = []
for row in response.rows:
    dados_paginas.append({
        "Data": row.dimension_values[0].value,
        "Título da Página": row.dimension_values[1].value,
        "Visualizações": int(row.metric_values[0].value)
    })

# Cria DataFrame Polars
df_paginas = pl.DataFrame(dados_paginas)

# Exibe no terminal
print("✅ Visualizações por título da página, dia a dia:")
print(df_paginas)

# (Opcional) Salvar arquivos
# df_paginas.write_csv("visualizacoes_titulo_por_dia.csv")
# df_paginas.write_ndjson("visualizacoes_titulo_por_dia.json")
