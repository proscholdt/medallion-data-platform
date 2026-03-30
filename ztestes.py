import os
import io
import polars as pl
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Azure
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"


# Conexão Azure Blob
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
silver_client = blob_service_client.get_container_client(CONTAINER_SILVER)
gold_client   = blob_service_client.get_container_client(CONTAINER_GOLD)

#Caminhos
pasta_origem = 'source_voomp/projetadas_voomp'
pasta_destino = 'source_voomp/projetadas_voomp'
nome_arquivo = "projetadasvoomp.parquet"

# caminho do arquivo dentro do container Silver
blob_caminho = f"{pasta_origem}/{nome_arquivo}"

print(f"🔍 Iniciando processamento do blob: {blob_caminho}")

try:
    # pega o blob na Silver
    blob_client = silver_client.get_blob_client(blob_caminho)

    # baixar em memória
    print("⬇️ Baixando blob da SILVER...")
    buffer = io.BytesIO()
    blob_client.download_blob().readinto(buffer)
    buffer.seek(0)

    #Lê como arquivo parquet
    print("📄 Lendo parquet em DataFrame Polars...")
    df=pl.read_parquet(buffer)

    # --- criar nova coluna "data" a partir de "Mês" (ex: jan/2026 -> 01/01/2026) ---
    MESES = {
        "jan": "01", "fev": "02", "mar": "03", "abr": "04",
        "mai": "05", "jun": "06", "jul": "07", "ago": "08",
        "set": "09", "out": "10", "nov": "11", "dez": "12",
    }

    def mes_ano_para_data(mes_ano: str) -> str:
        if mes_ano is None:
            return None
        mes_ano = mes_ano.strip()
        mes, ano = mes_ano.split("/")
        mes = mes.lower()
        mes_num = MESES.get(mes)
        if mes_num is None:
            return None
        return f"01/{mes_num}/{ano}"

    print("🛠️ Criando coluna 'Data' a partir de 'Mês'...")
    df = df.with_columns(
        pl.col("Mês").map_elements(mes_ano_para_data).alias("Data")
    )

    print("🛠️ Tratando coluna 'Valor Total' -> 'Valor'...")
    df = df.with_columns(
        pl.col('Valor Total')
        .str.replace_all(r"[R$\s]", "")   # remove "R$", espaços
        .str.replace_all(r"\.", "")       # remove separador de milhar
        .str.replace(",", ".")  # vírgula -> ponto decimal
        .cast(pl.Decimal(18,2))
        .alias('Valor')
    )

    print("🧾 Selecionando colunas finais...")
    df = df.select([
        'Data','Valor'
    ])

    print("✅ DataFrame final:")
    print(df.head())

    # --------- salvar resultado no container GOLD em parquet ---------- #
    gold_blob_caminho = f"{pasta_destino}/{nome_arquivo}"
    print(f"⬆️ Enviando resultado para GOLD: {gold_blob_caminho}")

    # Escreve df em parquet em memória
    out_buffer = io.BytesIO()
    df.write_parquet(out_buffer)
    out_buffer.seek(0)

    # faz upload para o GOLD
    gold_blob_client = gold_client.get_blob_client(gold_blob_caminho)
    gold_blob_client.upload_blob(out_buffer, overwrite=True)
    print("✅ Upload para GOLD concluído.")

    # Deletar arquivos da silver
    print("🗑️ Deletando blob original da SILVER...")
    silver_client.delete_blob(blob_caminho)
    print("✅ Blob deletado da SILVER.")

except Exception as e:
    print("❌ Erro durante o processamento do blob:")
    print(e)


