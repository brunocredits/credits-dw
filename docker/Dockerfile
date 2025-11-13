# ============================================================================
# Dockerfile - Credits Brasil ETL Processor
# Python 3.10+ para processamento de ETL
# ============================================================================

FROM python:3.10-slim

# Metadados
LABEL maintainer="Credits Brasil"
LABEL description="Container Python para processamento ETL do Data Warehouse"
LABEL version="1.0"

# Variáveis de ambiente
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Criar diretório de trabalho
WORKDIR /app

# Copiar requirements primeiro (para cache de layers)
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install great_expectations # Para vaildação de qualidade
# Copiar código da aplicação
COPY python/ ./python/

# Criar diretórios necessários
RUN mkdir -p /app/logs /app/data/input /app/data/processed

# Usuário não-root para segurança
RUN useradd -m -u 1000 etl_user && \
    chown -R etl_user:etl_user /app

USER etl_user

# Adicionar python/ ao PYTHONPATH
ENV PYTHONPATH=/app/python:$PYTHONPATH

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import psycopg2; print('OK')" || exit 1

# Comando padrão (pode ser sobrescrito no docker-compose)
CMD ["python", "-c", "print('ETL Container Ready. Use docker-compose exec to run scripts.')"]
