FROM python:3.13-slim

LABEL maintainer="IT Valley"
LABEL description="Data Engineering Platform — Medallion Architecture (Bronze → Silver → Gold)"

# Variáveis de ambiente
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=America/Sao_Paulo

WORKDIR /app

# Dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copiar código-fonte
COPY . .

# Comando padrão: ETL pipeline
CMD ["python", "datamart/1_Orchestrator_Gerneral/Orchestartor_General.py"]
