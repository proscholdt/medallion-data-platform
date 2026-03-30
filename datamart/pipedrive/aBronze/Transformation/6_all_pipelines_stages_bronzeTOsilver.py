import os
import json
import polars as pl
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from datetime import datetime
import logging

# Carregar variáveis do arquivo .env se existir
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Arquivo .env carregado")
except ImportError:
    print("⚠️ python-dotenv não instalado. Use: pip install python-dotenv")
except:
    print("⚠️ Arquivo .env não encontrado ou erro ao carregar")

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure configs - usando valores diretos se .env não funcionar
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME") or "saactivecampaign"
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY") or "wnqkmMpQcvRdMq33Vcx00f/xFk9C7cXRAf/zfrJtAg2GS6KEQZx242eQ/WkL6DI8Xw+x+EUUhFzLXr+AtTag8dQ=="
CONTAINER_NAME = "silver"  # SILVER

SOURCE_CONTAINER = "bronze"
SOURCE_FOLDER = "source_pipedrive/all_stages_and_pipelines"

DEST_FOLDER = "source_pipedrive/all_stages_and_pipelines"

# Debug das variáveis de ambiente
print(f"🔍 STORAGE_ACCOUNT_NAME: {'✅ Definido' if STORAGE_ACCOUNT_NAME else '❌ Não definido'}")
print(f"🔍 STORAGE_ACCOUNT_KEY: {'✅ Definido' if STORAGE_ACCOUNT_KEY else '❌ Não definido'}")

# Verificar se as variáveis de ambiente estão definidas
if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
    print("\n❌ VARIÁVEIS DE AMBIENTE FALTANDO!")
    print("\n💡 SOLUÇÕES:")
    print("1. Criar arquivo .env na raiz do projeto com:")
    print("   STORAGE_ACCOUNT_NAME=seu_account_name")
    print("   STORAGE_ACCOUNT_KEY=sua_account_key")
    print("\n2. Ou definir no PowerShell:")
    print("   $env:STORAGE_ACCOUNT_NAME = 'seu_account_name'")
    print("   $env:STORAGE_ACCOUNT_KEY = 'sua_account_key'")
    print("\n3. Ou definir permanentemente no Windows:")
    print("   setx STORAGE_ACCOUNT_NAME 'seu_account_name'")
    print("   setx STORAGE_ACCOUNT_KEY 'sua_account_key'")
    raise ValueError("❌ STORAGE_ACCOUNT_NAME ou STORAGE_ACCOUNT_KEY não estão definidas nas variáveis de ambiente")

# Conexão Azure
connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={STORAGE_ACCOUNT_NAME};"
    f"AccountKey={STORAGE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

try:
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    logger.info("✅ Conexão com Azure Blob Storage estabelecida")
except Exception as e:
    logger.error(f"❌ Erro ao conectar com Azure Blob Storage: {e}")
    raise

# Função para baixar TODOS os arquivos JSON do diretório
def download_all_json_files(container, folder):
    logger.info(f"🔍 Verificando container: {container}, pasta: {folder}")
    
    try:
        # Verificar se o container existe
        container_client = blob_service_client.get_container_client(container)
        
        # Testar se o container é acessível
        container_properties = container_client.get_container_properties()
        logger.info(f"✅ Container '{container}' encontrado. Última modificação: {container_properties.last_modified}")
        
    except Exception as e:
        logger.error(f"❌ Erro ao acessar container '{container}': {e}")
        return []
    
    try:
        # Listar blobs com tratamento de erro
        logger.info(f"📂 Listando blobs que começam com: {folder}")
        blobs_list = container_client.list_blobs(name_starts_with=folder)
        
        data_acumulado = []
        blob_count = 0
        
        for blob in blobs_list:
            blob_count += 1
            logger.info(f"📄 Blob encontrado #{blob_count}: {blob.name} (Tamanho: {blob.size} bytes)")
            
            if blob.name.endswith(".json"):
                try:
                    logger.info(f"⬇️ Baixando {blob.name}...")
                    blob_client = blob_service_client.get_blob_client(container=container, blob=blob.name)
                    
                    # Verificar se o blob existe e é acessível
                    blob_properties = blob_client.get_blob_properties()
                    logger.info(f"📊 Propriedades do blob: Tamanho={blob_properties.size}, Tipo={blob_properties.content_settings.content_type}")
                    
                    blob_data = blob_client.download_blob().readall()
                    
                    if not blob_data:
                        logger.warning(f"⚠️ Blob {blob.name} está vazio")
                        continue
                        
                    json_data = json.loads(blob_data.decode("utf-8"))
                    
                    if isinstance(json_data, list):
                        data_acumulado.extend(json_data)
                        logger.info(f"✅ Adicionados {len(json_data)} registros de {blob.name}")
                    elif isinstance(json_data, dict):
                        data_acumulado.append(json_data)
                        logger.info(f"✅ Adicionado 1 registro de {blob.name}")
                    else:
                        logger.warning(f"⚠️ Formato inesperado em {blob.name}: {type(json_data)}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Erro ao decodificar JSON de {blob.name}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"❌ Erro ao processar {blob.name}: {e}")
                    continue
            else:
                logger.info(f"⏭️ Ignorando {blob.name} (não é JSON)")
        
        if blob_count == 0:
            logger.warning(f"⚠️ Nenhum blob encontrado na pasta '{folder}'")
            
            # Vamos listar o que existe no container para debug
            logger.info("🔍 Listando TODOS os blobs do container para debug:")
            all_blobs = container_client.list_blobs()
            for i, blob in enumerate(all_blobs):
                if i < 10:  # Mostrar apenas os primeiros 10
                    logger.info(f"   - {blob.name}")
                elif i == 10:
                    logger.info("   - ... (mais blobs)")
                    break

        logger.info(f"✅ Total de arquivos JSON processados: {len(data_acumulado)} registros combinados de {blob_count} blobs.")
        return data_acumulado
        
    except Exception as e:
        logger.error(f"❌ Erro geral ao listar/processar blobs: {e}")
        logger.error(f"❌ Tipo do erro: {type(e).__name__}")
        return []

# Função para criar a dimensão única allStagesPipelines
def create_dim_all_stages_pipelines(data):
    logger.info("🏗️ Criando dimensão allStagesPipelines...")
    
    # Validar estrutura dos dados
    valid_items = []
    required_fields = ["stage_id", "stage_name", "stage_order_nr", "pipeline_id", "pipeline_name"]
    
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            logger.warning(f"⚠️ Item {i} não é um dicionário: {type(item)}")
            continue
            
        missing_fields = [field for field in required_fields if field not in item]
        if missing_fields:
            logger.warning(f"⚠️ Item {i} está faltando campos: {missing_fields}")
            continue
            
        valid_items.append({
            "stage_id": item["stage_id"],
            "stage_name": item["stage_name"],
            "stage_order_nr": item["stage_order_nr"],
            "pipeline_id": item["pipeline_id"],
            "pipeline_name": item["pipeline_name"]
        })
    
    if not valid_items:
        raise ValueError("❌ Nenhum item válido encontrado para processar")
    
    logger.info(f"✅ {len(valid_items)} itens válidos de {len(data)} totais")
    
    df_all = pl.DataFrame(valid_items).unique(subset=["stage_id"])

    # Criar coluna pk_allstages (concatenando como string)
    df_all = df_all.with_columns(
        (
            df_all["stage_id"].cast(str)
            + "|"
            + df_all["pipeline_id"].cast(str)
        ).alias("pk_allstages")
    )

    logger.info(f"✅ Dimensão allStagesPipelines criada com {df_all.height} linhas")
    return df_all

# Função para enviar para Azure Blob
def upload_parquet(df, dest_folder, blob_name):
    try:
        logger.info(f"📤 Enviando {blob_name} para Azure...")
        buffer = BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=f"{dest_folder}/{blob_name}"
        )
        blob_client.upload_blob(buffer, overwrite=True)
        logger.info(f"✅ Upload feito: {dest_folder}/{blob_name}")
    except Exception as e:
        logger.error(f"❌ Erro no upload: {e}")
        raise

# 🏁 ORQUESTRAÇÃO
if __name__ == "__main__":
    logger.info("🚀 Iniciando orquestração...")

    try:
        # Baixar TODOS os JSON da bronze
        stages_pipelines = download_all_json_files(SOURCE_CONTAINER, SOURCE_FOLDER)

        if stages_pipelines:
            # Criar a dimensão única
            df_all_stages_pipelines = create_dim_all_stages_pipelines(stages_pipelines)

            # Criar nome dinâmico com timestamp
            now = datetime.now()
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
            parquet_file_name = f"all_stages_and_pipelines_{timestamp_str}.parquet"

            # Enviar para Azure (SILVER agora)
            upload_parquet(df_all_stages_pipelines, DEST_FOLDER, parquet_file_name)

            logger.info("🏁 Orquestração concluída com sucesso 🚀")
        else:
            logger.warning("⚠️ Nenhum arquivo JSON encontrado para processar.")

    except Exception as e:
        logger.error(f"❌ Erro na orquestração: {e}")
        raise










# import os
# import json
# import polars as pl
# from azure.storage.blob import BlobServiceClient
# from io import BytesIO
# from datetime import datetime

# # Azure configs
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# CONTAINER_NAME = "silver"  # SILVER

# SOURCE_CONTAINER = "bronze"
# SOURCE_FOLDER = "source_pipedrive/all_stages_and_pipelines"

# DEST_FOLDER = "source_pipedrive/all_stages_and_pipelines"

# # Conexão Azure
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# # Função para baixar TODOS os arquivos JSON do diretório
# def download_all_json_files(container, folder):
#     container_client = blob_service_client.get_container_client(container)
#     blobs_list = container_client.list_blobs(name_starts_with=folder)
#     data_acumulado = []

#     for blob in blobs_list:
#         if blob.name.endswith(".json"):
#             print(f"⬇️ Baixando {blob.name}...")
#             blob_client = blob_service_client.get_blob_client(container=container, blob=blob.name)
#             blob_data = blob_client.download_blob().readall()
#             json_data = json.loads(blob_data.decode("utf-8"))
#             data_acumulado.extend(json_data)

#     print(f"✅ Total de arquivos JSON processados: {len(data_acumulado)} registros combinados.")
#     return data_acumulado

# # Função para criar a dimensão única allStagesPipelines
# def create_dim_all_stages_pipelines(data):
#     df_all = pl.DataFrame([
#         {
#             "stage_id": item["stage_id"],
#             "stage_name": item["stage_name"],
#             "stage_order_nr": item["stage_order_nr"],
#             "pipeline_id": item["pipeline_id"],
#             "pipeline_name": item["pipeline_name"]
#         }
#         for item in data
#     ]).unique(subset=["stage_id"])

#     # Criar coluna pk_allstages (concatenando como string)
#     df_all = df_all.with_columns(
#         (
#             df_all["stage_id"].cast(str)
#             + "|"
#             + df_all["pipeline_id"].cast(str)
#         ).alias("pk_allstages")
#     )

#     print("✅ Dimensão allStagesPipelines criada com pk_allstages")
#     return df_all

# # Função para enviar para Azure Blob
# def upload_parquet(df, dest_folder, blob_name):
#     buffer = BytesIO()
#     df.write_parquet(buffer)
#     buffer.seek(0)
#     blob_client = blob_service_client.get_blob_client(
#         container=CONTAINER_NAME,
#         blob=f"{dest_folder}/{blob_name}"
#     )
#     blob_client.upload_blob(buffer, overwrite=True)
#     print(f"✅ Upload feito: {dest_folder}/{blob_name}")

# # 🏁 ORQUESTRAÇÃO
# print("🚀 Iniciando orquestração...")

# # Baixar TODOS os JSON da bronze
# stages_pipelines = download_all_json_files(SOURCE_CONTAINER, SOURCE_FOLDER)

# if stages_pipelines:
#     # Criar a dimensão única
#     df_all_stages_pipelines = create_dim_all_stages_pipelines(stages_pipelines)

#     # Criar nome dinâmico com timestamp
#     now = datetime.now()
#     timestamp_str = now.strftime("%Y%m%d_%H%M%S")
#     parquet_file_name = f"all_stages_and_pipelines_{timestamp_str}.parquet"

#     # Enviar para Azure (SILVER agora)
#     upload_parquet(df_all_stages_pipelines, DEST_FOLDER, parquet_file_name)

#     print("🏁 Orquestração concluída com sucesso 🚀")
# else:
#     print("⚠️ Nenhum arquivo JSON encontrado para processar.")







# # ************************************************************************************************



# # ==========================================================
# # Script: Consolidação Estágios/Pipelines (Pipedrive JSON -> Parquet)
# # ==========================================================
# # Este script busca TODOS os arquivos JSON disponíveis na pasta 
# # 'all_stages_and_pipelines' da camada Bronze (Azure Blob Storage), 
# # consolida os dados dos estágios e pipelines em um único DataFrame 
# # Polars e salva um arquivo Parquet na camada Silver.


# # ************************************************************************************************


# import os
# import json
# import polars as pl
# from azure.storage.blob import BlobServiceClient
# from io import BytesIO
# from datetime import datetime

# # Azure configs
# STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
# STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
# CONTAINER_NAME = "silver"  # SILVER

# SOURCE_CONTAINER = "bronze"
# SOURCE_FOLDER = "source_pipedrive/all_stages_and_pipelines"

# DEST_FOLDER = "source_pipedrive/all_stages_and_pipelines"

# # Conexão Azure
# connection_string = (
#     f"DefaultEndpointsProtocol=https;"
#     f"AccountName={STORAGE_ACCOUNT_NAME};"
#     f"AccountKey={STORAGE_ACCOUNT_KEY};"
#     f"EndpointSuffix=core.windows.net"
# )
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# # Função para baixar TODOS os arquivos JSON do diretório
# def download_all_json_files(container, folder):
#     container_client = blob_service_client.get_container_client(container)

#     print(f"📂 Listando blobs em '{container}' com prefixo '{folder}'")

#     try:
#         blobs_list = list(container_client.list_blobs(name_starts_with=folder))
#     except Exception as e:
#         print(f"❌ Erro ao listar blobs: {e}")
#         raise

#     print(f"📦 Total de arquivos encontrados: {len(blobs_list)}")

#     data_acumulado = []

#     for blob in blobs_list:
#         if blob.name.endswith(".json"):
#             print(f"⬇️ Baixando {blob.name}...")
#             blob_client = blob_service_client.get_blob_client(container=container, blob=blob.name)
#             blob_data = blob_client.download_blob().readall()
#             json_data = json.loads(blob_data.decode("utf-8"))
#             data_acumulado.extend(json_data)

#     print(f"✅ Total de registros combinados: {len(data_acumulado)}")
#     return data_acumulado


# # Função para criar a dimensão única allStagesPipelines
# def create_dim_all_stages_pipelines(data):
#     df_all = pl.DataFrame([
#         {
#             "stage_id": item["stage_id"],
#             "stage_name": item["stage_name"],
#             "stage_order_nr": item["stage_order_nr"],
#             "pipeline_id": item["pipeline_id"],
#             "pipeline_name": item["pipeline_name"]
#         }
#         for item in data
#     ]).unique(subset=["stage_id"])

#     # Criar coluna pk_allstages (concatenando como string)
#     df_all = df_all.with_columns(
#         (
#             df_all["stage_id"].cast(str)
#             + "|"
#             + df_all["pipeline_id"].cast(str)
#         ).alias("pk_allstages")
#     )

#     print("✅ Dimensão allStagesPipelines criada com pk_allstages")
#     return df_all

# # Função para enviar para Azure Blob
# def upload_parquet(df, dest_folder, blob_name):
#     buffer = BytesIO()
#     df.write_parquet(buffer)
#     buffer.seek(0)
#     blob_client = blob_service_client.get_blob_client(
#         container=CONTAINER_NAME,
#         blob=f"{dest_folder}/{blob_name}"
#     )
#     blob_client.upload_blob(buffer, overwrite=True)
#     print(f"✅ Upload feito: {dest_folder}/{blob_name}")

# # 🏁 ORQUESTRAÇÃO
# print("🚀 Iniciando orquestração...")

# # Baixar TODOS os JSON da bronze
# stages_pipelines = download_all_json_files(SOURCE_CONTAINER, SOURCE_FOLDER)

# if stages_pipelines:
#     # Criar a dimensão única
#     df_all_stages_pipelines = create_dim_all_stages_pipelines(stages_pipelines)

#     # Criar nome dinâmico com timestamp
#     now = datetime.now()
#     timestamp_str = now.strftime("%Y%m%d_%H%M%S")
#     parquet_file_name = f"all_stages_and_pipelines_{timestamp_str}.parquet"

#     # Enviar para Azure (SILVER agora)
#     upload_parquet(df_all_stages_pipelines, DEST_FOLDER, parquet_file_name)

#     print("🏁 Orquestração concluída com sucesso 🚀")
# else:
#     print("⚠️ Nenhum arquivo JSON encontrado para processar.")
