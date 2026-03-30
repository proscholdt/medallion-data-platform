import polars as pl
import os
from io import BytesIO
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

print("=" * 60)
print("INICIANDO PROCESSAMENTO DE LEADS")
print("=" * 60)

try:
    print("\n[1/7] Carregando variáveis de ambiente...")
    load_dotenv()

    # Configuração Azure Blob Storage
    STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
    STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
    CONTAINER_NAME = "bronze"
    CAMINHO_PASTA_BLOB = "source_activeCampaign/leads"

    if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
        raise ValueError("Variáveis de ambiente STORAGE_ACCOUNT_NAME ou STORAGE_ACCOUNT_KEY não configuradas!")

    print(f"✓ Storage Account: {STORAGE_ACCOUNT_NAME}")
    print(f"✓ Container: {CONTAINER_NAME}")

    print("\n[2/7] Conectando ao Azure Blob Storage...")
    try:
        # Conexão com Azure Blob Storage
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            credential=STORAGE_ACCOUNT_KEY
        )
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        print("✓ Conexão estabelecida!")
    except Exception as e:
        print(f"✗ Erro ao conectar ao Azure Blob Storage: {str(e)}")
        raise

    print(f"\n[3/7] Listando arquivos em: {CAMINHO_PASTA_BLOB}...")
    try:
        # Listar blobs no diretório para encontrar o arquivo CSV
        blobs = container_client.list_blobs(name_starts_with=CAMINHO_PASTA_BLOB)
        csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]

        if not csv_files:
            print(f"✗ Nenhum arquivo CSV encontrado em {CAMINHO_PASTA_BLOB}")
            print(f"Blobs disponíveis:")
            blobs = container_client.list_blobs(name_starts_with=CAMINHO_PASTA_BLOB)
            for blob in blobs:
                print(f"  - {blob.name}")
            raise FileNotFoundError(f"Nenhum arquivo CSV em {CAMINHO_PASTA_BLOB}")

        # Usar o primeiro arquivo CSV encontrado
        arquivo_blob = csv_files[0]
        print(f"✓ Arquivos CSV encontrados: {len(csv_files)}")
        print(f"✓ Usando arquivo: {arquivo_blob}")
    except FileNotFoundError as e:
        print(f"✗ Erro ao listar arquivos: {str(e)}")
        raise
    except Exception as e:
        print(f"✗ Erro inesperado ao listar blobs: {str(e)}")
        raise

    print(f"\n[4/7] Lendo arquivo do blob...")
    try:
        # Ler arquivo do blob
        blob_client = container_client.get_blob_client(arquivo_blob)
        blob_data = blob_client.download_blob().readall()
        df = pl.read_csv(BytesIO(blob_data), separator=",")
        print(f"✓ Arquivo lido com sucesso!")
        print(f"✓ Dimensões: {df.shape[0]} linhas x {df.shape[1]} colunas")
    except Exception as e:
        print(f"✗ Erro ao ler arquivo do blob: {str(e)}")
        raise

    print(f"\n[5/7] Processando dados (selecionando e renomeando colunas)...")
    try:
        df_leadsActive = df.select(
            [
                pl.col('firstName').alias('nome_lead_Active'),
                pl.col('email').alias('email_lead_Active'),
                pl.col("cf__utm_campaign").alias('umt_campaign_Active'),
                pl.col("cf__utm_source").alias('utm_source_Active'),
                pl.col("cf__utm_medium").alias('utm_medium_Active'),
                pl.col("cf__utm_content").alias('utm_content_Active'),
                pl.col("cdate").alias('cdate_Active')
            ]
        )
        print(f"✓ Dados processados!")
        print(f"✓ Colunas: {df_leadsActive.columns}")
        print(f"\nPreview dos dados:")
        print(df_leadsActive)
    except Exception as e:
        print(f"✗ Erro ao processar dados: {str(e)}")
        print(f"  Colunas disponíveis: {df.columns}")
        raise

    # Salvar resultado na camada SILVER
    CONTAINER_SILVER = "silver"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    CAMINHO_SILVER = f"source_activeCampaign/leads/leads_processados_{timestamp}.parquet"

    print(f"\n[6/7] Convertendo para Parquet...")
    try:
        # Converter dataframe para Parquet em bytes
        parquet_buffer = BytesIO()
        df_leadsActive.write_parquet(parquet_buffer)
        parquet_data = parquet_buffer.getvalue()
        print(f"✓ Arquivo convertido para Parquet ({len(parquet_data)} bytes)")
    except Exception as e:
        print(f"✗ Erro ao converter para Parquet: {str(e)}")
        raise

    print(f"\n[7/7] Conectando ao container SILVER e fazendo upload...")
    try:
        # Conectar ao container silver
        container_silver = blob_service_client.get_container_client(CONTAINER_SILVER)

        # Upload para Azure Blob Storage
        blob_silver = container_silver.get_blob_client(CAMINHO_SILVER)
        blob_silver.upload_blob(parquet_data, overwrite=True)
        print(f"✓ Upload concluído!")
    except Exception as e:
        print(f"✗ Erro ao fazer upload: {str(e)}")
        raise

    print(f"\n[8/8] Movendo arquivo CSV processado para leads_carregados...")
    try:
        # Caminho de destino na bronze (manter a estrutura de pastas)
        CAMINHO_LEADS_CARREGADOS = f"source_activeCampaign/carregados_leads/{arquivo_blob.split('/')[-1]}"
        
        # Baixar o arquivo CSV original
        blob_source = container_client.get_blob_client(arquivo_blob)
        csv_download = blob_source.download_blob().readall()
        print(f"  Arquivo baixado: {len(csv_download)} bytes")
        
        # Upload para a pasta leads_carregados
        blob_dest = container_client.get_blob_client(CAMINHO_LEADS_CARREGADOS)
        blob_dest.upload_blob(csv_download, overwrite=True)
        print(f"✓ Arquivo copiado para: bronze/{CAMINHO_LEADS_CARREGADOS}")
        
        # Verificar se o arquivo foi realmente enviado
        print(f"  Verificando se arquivo foi enviado com sucesso...")
        try:
            verify_download = blob_dest.download_blob().readall()
            if len(verify_download) == len(csv_download):
                print(f"✓ Verificação OK! Arquivo enviado corretamente ({len(verify_download)} bytes)")
                
                # Agora deletar o arquivo original
                blob_source.delete_blob()
                print(f"✓ Arquivo original deletado: bronze/{arquivo_blob}")
            else:
                raise Exception(f"Tamanho do arquivo não coincide! Original: {len(csv_download)} bytes, Verificado: {len(verify_download)} bytes")
        except Exception as verify_error:
            print(f"✗ FALHA NA VERIFICAÇÃO: {str(verify_error)}")
            print(f"  Arquivo original NOT foi deletado por segurança!")
            raise
        
    except Exception as e:
        print(f"✗ Erro ao mover arquivo para leads_carregados: {str(e)}")
        raise

    print("\n" + "=" * 60)
    print(f"✓ SUCESSO! Processamento concluído:")
    print(f"  - Parquet salvo em: silver/{CAMINHO_SILVER}")
    print(f"  - CSV copiado para: bronze/{CAMINHO_LEADS_CARREGADOS}")
    print("=" * 60)

except ValueError as e:
    print(f"\n✗ ERRO DE CONFIGURAÇÃO: {str(e)}")
    print("  Verifique o arquivo .env com as variáveis:")
    print("  - STORAGE_ACCOUNT_NAME")
    print("  - STORAGE_ACCOUNT_KEY")
    exit(1)
except FileNotFoundError as e:
    print(f"\n✗ ERRO DE ARQUIVO NÃO ENCONTRADO: {str(e)}")
    exit(1)
except Exception as e:
    print(f"\n✗ ERRO GERAL: {str(e)}")
    print(f"  Tipo: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    exit(1)