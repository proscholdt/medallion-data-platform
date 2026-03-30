import os
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import polars as pl
from io import BytesIO

# Carregar variáveis de ambiente
load_dotenv()

# Azure configs
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "silver"
SOURCE_FOLDER = "source_pipedrive/all_stages_and_pipelines_carregados/"
DEST_PATH = "source_pipedrive/scd2_dim_stage/scd2_dim_stage.parquet"

# Conexão com Azure Blob
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Listar arquivos .parquet ordenados por timestamp no nome
blobs = container_client.list_blobs(name_starts_with=SOURCE_FOLDER)
arquivos = sorted(
    [b.name for b in blobs if b.name.endswith(".parquet")],
    key=lambda x: "_".join(os.path.basename(x).replace(".parquet", "").split("_")[-2:])
)

# Função para ler snapshot e extrair timestamp do nome
def ler_snapshot(blob_name):
    blob_data = container_client.download_blob(blob_name).readall()
    nome = os.path.basename(blob_name).replace(".parquet", "")
    timestamp_str = "_".join(nome.split("_")[-2:])
    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
    df = pl.read_parquet(BytesIO(blob_data)).with_columns([
        pl.lit(timestamp).alias("snapshot_time")
    ])
    return df, timestamp

# Inicialização
scd2_registros = []
df_anterior = None
ts_anterior = None

# Chave natural para comparar versões de stage
chave = ["stage_name", "pipeline_id", "stage_order_nr"]

# Comparar arquivos sequencialmente
for idx in range(len(arquivos)):
    blob_atual = arquivos[idx]
    df_atual, ts_atual = ler_snapshot(blob_atual)

    if df_anterior is None:
        # Primeiro snapshot: tudo entra como novo
        for row in df_atual.iter_rows(named=True):
            row_out = row.copy()
            row_out["dt_inicio_vigencia"] = ts_atual
            row_out["dt_fim_vigencia"] = None
            scd2_registros.append(row_out)
    else:
        # Comparar com snapshot anterior
        df_merge = df_anterior.join(df_atual, on=chave, how="outer", suffix="_novo")

        for row in df_merge.iter_rows(named=True):
            antigo = {k: row.get(k) for k in chave + ["stage_id", "pipeline_name"]}
            novo = {k: row.get(k + "_novo") for k in ["stage_id", "pipeline_name"]}

            # Se qualquer campo relevante mudou
            if antigo["stage_id"] != novo["stage_id"] or antigo["pipeline_name"] != novo["pipeline_name"]:
                # Fecha vigência do anterior, se existir
                for r in scd2_registros:
                    if all(r[k] == row.get(k) for k in chave) and r["dt_fim_vigencia"] is None:
                        r["dt_fim_vigencia"] = ts_atual

                # Cria nova versão
                if row.get("stage_id_novo") is not None:
                    row_out = {
                        "stage_id": row.get("stage_id_novo"),
                        "stage_name": row.get("stage_name"),
                        "stage_order_nr": row.get("stage_order_nr"),
                        "pipeline_id": row.get("pipeline_id"),
                        "pipeline_name": row.get("pipeline_name_novo"),
                        "dt_inicio_vigencia": ts_atual,
                        "dt_fim_vigencia": None
                    }
                    scd2_registros.append(row_out)

    df_anterior = df_atual
    ts_anterior = ts_atual

# Fecha os registros vigentes com 9999-12-31
fim_aberto = datetime(9999, 12, 31, 23, 59, 59)
for r in scd2_registros:
    if r["dt_fim_vigencia"] is None:
        r["dt_fim_vigencia"] = fim_aberto

# Converter para DataFrame Polars
df_final = pl.DataFrame(scd2_registros).with_columns([
    (pl.col("stage_id").cast(pl.Utf8) + "|" + pl.col("pipeline_id").cast(pl.Utf8)).alias("pk_allstages")
])

# Salvar como Parquet no Azure
buffer = BytesIO()
df_final.write_parquet(buffer)
buffer.seek(0)

blob_client = container_client.get_blob_client(DEST_PATH)
blob_client.upload_blob(buffer, overwrite=True)

print("✅ Arquivo salvo com sucesso em:", DEST_PATH)
