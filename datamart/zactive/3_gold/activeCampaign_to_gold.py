import os
import logging
import polars as pl
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from datetime import datetime

load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === AZURE CONFIGURATION ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "silver"
CAMINHO_PASTA_BLOB = "source_activeCampaign/leads"

if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
    raise ValueError("STORAGE_ACCOUNT_NAME and STORAGE_ACCOUNT_KEY environment variables must be set")


def read_parquet_from_azure(blob_path: str) -> pl.DataFrame:
    """
    Lê um arquivo parquet do Azure Blob Storage.
    """
    try:
        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={STORAGE_ACCOUNT_NAME};"
            f"AccountKey={STORAGE_ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Baixar blob para memória
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, 
            blob=blob_path
        )
        
        blob_data = blob_client.download_blob().readall()
        buffer = BytesIO(blob_data)
        
        # Ler como parquet usando Polars
        df = pl.read_parquet(buffer)
        logger.info(f"✓ Lido: {blob_path} ({len(df)} linhas)")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler {blob_path}: {e}")
        raise


def move_blob_to_archive(blob_path: str, source_container: str = "silver", destination_folder: str = "carregados_leads") -> None:
    """
    Move um arquivo para pasta de arquivamento na silver.
    """
    try:
        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={STORAGE_ACCOUNT_NAME};"
            f"AccountKey={STORAGE_ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Obter o nome do arquivo
        file_name = blob_path.split("/")[-1]
        destination_path = f"{destination_folder}/{file_name}"

        # Copiar arquivo para novo local
        source_blob = blob_service_client.get_blob_client(
            container=source_container,
            blob=blob_path
        )
        destination_blob = blob_service_client.get_blob_client(
            container=source_container,
            blob=destination_path
        )

        # Download e upload para novo local
        blob_data = source_blob.download_blob().readall()
        destination_blob.upload_blob(blob_data, overwrite=True)

        # Deletar arquivo original
        source_blob.delete_blob()

        logger.info(f"✓ Arquivo movido: {blob_path} → {destination_path}")
    except Exception as e:
        logger.error(f"Erro ao mover {blob_path}: {e}")
        raise


def write_parquet_to_azure(df: pl.DataFrame, blob_path: str, container: str = "gold") -> None:
    """
    Escreve um dataframe como arquivo parquet no Azure Blob Storage.
    """
    try:
        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={STORAGE_ACCOUNT_NAME};"
            f"AccountKey={STORAGE_ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Converter dataframe para bytes
        buffer = BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)

        # Upload para blob
        blob_client = blob_service_client.get_blob_client(
            container=container,
            blob=blob_path
        )
        blob_client.upload_blob(buffer.getvalue(), overwrite=True)
        logger.info(f"✓ Salvo em: {container}/{blob_path} ({len(df)} linhas)")
    except Exception as e:
        logger.error(f"Erro ao salvar {blob_path}: {e}")
        raise


def list_parquet_files() -> list[str]:
    """
    Lista todos os arquivos .parquet da pasta silver/activecampaign/lead
    """
    try:
        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={STORAGE_ACCOUNT_NAME};"
            f"AccountKey={STORAGE_ACCOUNT_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        
        parquet_files = []
        blobs = container_client.list_blobs(name_starts_with=CAMINHO_PASTA_BLOB)
        
        for blob in blobs:
            if blob.name.endswith(".parquet"):
                parquet_files.append(blob.name)
        
        logger.info(f"✓ {len(parquet_files)} arquivo(s) parquet encontrado(s)")
        for f in parquet_files:
            logger.info(f"  - {f}")
        
        return parquet_files
    except Exception as e:
        logger.error(f"Erro ao listar arquivos: {e}")
        raise


def main():
    try:
        logger.info("=" * 60)
        logger.info("LENDO ARQUIVOS PARQUET DO AZURE - SILVER/LEAD")
        logger.info("=" * 60)
        
        # 1) Listar todos os arquivos parquet
        logger.info(f"\n📁 Procurando parquets em: {CONTAINER_NAME}/{CAMINHO_PASTA_BLOB}")
        parquet_files = list_parquet_files()
        
        if not parquet_files:
            logger.warning("⚠ Nenhum arquivo parquet encontrado!")
            return
        
        # 2) Ler todos os parquets e consolidar
        logger.info(f"\n📖 Lendo {len(parquet_files)} arquivo(s)...")
        dataframes = []
        
        for file_path in parquet_files:
            try:
                df = read_parquet_from_azure(file_path)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Falha ao processar {file_path}: {e}")
                continue
        
        if not dataframes:
            logger.error("❌ Nenhum arquivo foi lido com sucesso!")
            return
        
        # 3) Consolidar todos os dataframes
        logger.info(f"\n🔗 Consolidando {len(dataframes)} dataframe(s)...")
        df_final = pl.concat(dataframes)

        logger.info(f"✓ Consolidado com sucesso!")
        logger.info(f"  - Total de linhas: {len(df_final)}")
        logger.info(f"  - Total de colunas: {len(df_final.columns)}")
        logger.info(f"\n📋 Colunas: {df_final.columns}")

        # 3.1) Renomear coluna umt_campaign_Active para utm_campaign_Active
        if "umt_campaign_Active" in df_final.columns:
            logger.info(f"\n📝 Renomeando coluna 'umt_campaign_Active' para 'utm_campaign_Active'...")
            df_final = df_final.rename({"umt_campaign_Active": "utm_campaign_Active"})
            logger.info(f"✓ Coluna renomeada com sucesso!")
        
        # 3.2) Tratar coluna de data (cdate_Active)
        if "cdate_Active" in df_final.columns:
            logger.info(f"\n📅 Tratando coluna de data 'cdate_Active'...")
            df_final = df_final.with_columns(
                pl.col("cdate_Active")
                .str.strip_chars()  # Remove espaços em branco
                .str.slice(0, 10)  # Pega apenas os primeiros 10 caracteres
            )
            logger.info(f"✓ Coluna de data tratada com sucesso!")
        
        # 4) Eliminar duplicados baseado em email
        logger.info(f"\n🔄 Eliminando duplicados baseado em email...")
        linhas_antes = len(df_final)
        
        # Verificar se existe coluna de email
        email_cols = [col for col in df_final.columns if col.lower() in ['email', 'email_address', 'emailaddress', 'mail']]
        
        if email_cols:
            email_col = email_cols[0]
            logger.info(f"  - Usando coluna: '{email_col}'")
            
            # Remover linhas onde email é nulo/vazio
            df_final = df_final.filter(pl.col(email_col).is_not_null() & (pl.col(email_col) != ""))
            
            # Remover duplicatas, mantendo a primeira ocorrência
            df_final = df_final.unique(subset=[email_col], keep="first")
            
            linhas_depois = len(df_final)
            removidos = linhas_antes - linhas_depois
            logger.info(f"✓ Duplicados removidos: {removidos} linhas")
            logger.info(f"  - Antes: {linhas_antes} linhas")
            logger.info(f"  - Depois: {linhas_depois} linhas")
        else:
            logger.warning(f"⚠ Nenhuma coluna de email encontrada! Colunas disponíveis: {df_final.columns}")
        
        # 5) Exibir primeiras linhas
        logger.info(f"\n📊 Amostra dos dados (primeiras 5 linhas):")
        logger.info(f"\n{df_final.head()}")
        
        # 5) Exibir informações estatísticas
        logger.info(f"\n📈 Tipos de dados:")
        for col_name, col_type in zip(df_final.columns, df_final.schema.values()):
            logger.info(f"  - {col_name}: {col_type}")

        # 6) Salvar resultado na gold
        logger.info(f"\n💾 Salvando resultado na gold...")
        blob_path = "source_activeCampaign/leads/leadsActive.parquet"
        write_parquet_to_azure(df_final, blob_path, container="gold")

        # 7) Mover arquivos processados para carregados_leads
        logger.info(f"\n🗂️ Movendo arquivos processados para carregados_leads...")
        for file_path in parquet_files:
            try:
                move_blob_to_archive(file_path, source_container="silver", destination_folder="source_activeCampaign/carregados_leads")
            except Exception as e:
                logger.warning(f"⚠ Tentativa de mover {file_path} falhou (pode já estar arquivado): {e}")
                continue

        logger.info(f"\n✅ Leitura e processamento concluído com sucesso!")
        
        return df_final
        
    except Exception as e:
        logger.error(f"❌ Erro durante execução: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
