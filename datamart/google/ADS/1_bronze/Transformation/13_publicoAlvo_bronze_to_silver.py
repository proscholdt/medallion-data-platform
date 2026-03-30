import polars as pl
from azure.storage.blob import BlobServiceClient
from io import BytesIO, StringIO
from dotenv import load_dotenv
import os

# === CARREGAR VARIÁVEIS DE AMBIENTE ===
load_dotenv()

# === VARIÁVEIS DE CONEXÃO ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_BRONZE = "bronze"
CONTAINER_SILVER = "silver"
CAMINHO_BRONZE = "source_google/publico_alvo"
CAMINHO_SILVER = "source_google/publico_alvo"

# === CONEXÃO COM AZURE BLOB STORAGE ===
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
bronze_container = blob_service_client.get_container_client(CONTAINER_BRONZE)
silver_container = blob_service_client.get_container_client(CONTAINER_SILVER)

# === COLUNAS ESPERADAS ===
colunas_float = ["conversoes"]
colunas_int = ["impressoes", "cliques"]
coluna_data = "data"
coluna_publico = "publicos_do_grupo"

# === FUNÇÕES AUXILIARES ===
def detectar_separador(texto: str) -> str:
    return ";" if texto.count(";") > texto.count(",") else ","

def ler_csv_com_schema(csv_text: str) -> pl.DataFrame:
    sep = detectar_separador(csv_text)
    schema_overrides = {
        coluna_publico: pl.Utf8,
        coluna_data: pl.Utf8,
    }
    for c in colunas_int + colunas_float:
        schema_overrides[c] = pl.Utf8

    df = pl.read_csv(
        StringIO(csv_text),
        separator=sep,
        schema_overrides=schema_overrides,
        ignore_errors=False,
        null_values=["", "null", "NULL", "None", "NaN"]
    )
    return df

def normalizar_tipos(df: pl.DataFrame) -> pl.DataFrame:
    exprs = []

    if coluna_publico in df.columns:
        exprs.append(
            pl.col(coluna_publico)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .alias(coluna_publico)
        )

    if coluna_data in df.columns:
        exprs.append(
            pl.col(coluna_data)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .alias(coluna_data)
        )

    for c in colunas_int:
        if c in df.columns:
            exprs.append(
                pl.col(c)
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .str.replace_all(r"[^\d\-]", "")
                .cast(pl.Int64, strict=False)
                .fill_null(0)
                .alias(c)
            )

    for c in colunas_float:
        if c in df.columns:
            exprs.append(
                pl.col(c)
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .str.replace(",", ".", literal=False)
                .str.replace_all(r"[^0-9\.\-]", "")
                .cast(pl.Float64, strict=False)
                .fill_null(0.0)
                .alias(c)
            )

    if exprs:
        df = df.with_columns(exprs)
    return df

def gerar_id_publico_por_join(df: pl.DataFrame) -> pl.DataFrame:
    if coluna_publico not in df.columns:
        return df

    # Tabela de mapeamento: valores únicos não vazios com id incremental a partir de 1
    mapa = (
        df.select(
            pl.col(coluna_publico)
            .cast(pl.Utf8)
            .fill_null("")
            .str.strip_chars()
            .alias(coluna_publico)
        )
        .unique()
        .filter(pl.col(coluna_publico) != "")
        .sort(coluna_publico)
        .with_row_index(name="id_publico", offset=1)
    )

    # Join para preencher id_publico; valores vazios/nulos recebem 0
    df = (
        df
        .with_columns(
            pl.col(coluna_publico)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
            .alias(coluna_publico)
        )
        .join(mapa, on=coluna_publico, how="left")
        .with_columns(pl.col("id_publico").fill_null(0))
    )

    return df

# === LISTAR E PROCESSAR CSVs DO BRONZE ===
blobs = bronze_container.list_blobs(name_starts_with=CAMINHO_BRONZE + "/")

for blob in blobs:
    blob_name = blob.name
    if not blob_name.endswith(".csv"):
        continue

    print(f"Processando: {blob_name}")

    blob_client = bronze_container.get_blob_client(blob_name)
    csv_bytes = blob_client.download_blob().readall()
    try:
        csv_text = csv_bytes.decode("utf-8")
    except UnicodeDecodeError:
        csv_text = csv_bytes.decode("latin-1")

    df = ler_csv_com_schema(csv_text)
    df = normalizar_tipos(df)
    df = gerar_id_publico_por_join(df)

    parquet_buffer = BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)

    nome_arquivo_parquet = blob_name.split("/")[-1].replace(".csv", ".parquet")
    caminho_silver = f"{CAMINHO_SILVER}/{nome_arquivo_parquet}"

    silver_blob = silver_container.get_blob_client(caminho_silver)
    silver_blob.upload_blob(parquet_buffer.getvalue(), overwrite=True)

    print(f"Enviado para Silver: {caminho_silver}")

print("Concluído: todos os arquivos convertidos e enviados para Silver.")
