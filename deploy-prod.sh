#!/bin/bash
# ============================================================================
# Credits DW - Deployment Script for Production
# Secure environment variables handling
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}🚀 Credits DW - Production Deployment${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}❌ Este script deve ser executado como root ou com sudo${NC}"
    exit 1
fi

# Define production .env path
PROD_ENV_PATH="/opt/credits-dw/.env"
PROD_DIR="/opt/credits-dw"

# Create production directory if doesn't exist
if [ ! -d "$PROD_DIR" ]; then
    echo -e "${BLUE}📁 Criando diretório de produção...${NC}"
    mkdir -p "$PROD_DIR"
fi

# Check if production .env exists
if [ ! -f "$PROD_ENV_PATH" ]; then
    echo -e "${YELLOW}⚠️  Arquivo .env de produção não encontrado em $PROD_ENV_PATH${NC}"
    echo -e "${YELLOW}📝 Criando template...${NC}"
    
    cat > "$PROD_ENV_PATH" << 'EOF'
# Database Configuration
DB_HOST=creditsdw.postgres.database.azure.com
DB_PORT=5432
DB_NAME=creditsdw
DB_USER=creditsdw
DB_PASSWORD=CHANGE_ME

# Application
LOG_LEVEL=INFO
EOF
    
    echo -e "${RED}❌ Configure as credenciais em $PROD_ENV_PATH${NC}"
    echo -e "${YELLOW}   Depois execute este script novamente${NC}"
    exit 1
fi

# Set secure permissions
echo -e "${BLUE}🔒 Configurando permissões seguras...${NC}"
chown root:docker "$PROD_ENV_PATH" 2>/dev/null || chown root:root "$PROD_ENV_PATH"
chmod 640 "$PROD_ENV_PATH"

# Validate required variables
echo -e "${BLUE}✅ Validando variáveis de ambiente...${NC}"
required_vars=("DB_HOST" "DB_PORT" "DB_NAME" "DB_USER" "DB_PASSWORD")

# Load .env
set -a
source "$PROD_ENV_PATH"
set +a

missing_vars=()
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ] || [ "${!var}" == "CHANGE_ME" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo -e "${RED}❌ Variáveis não configuradas: ${missing_vars[*]}${NC}"
    echo -e "${YELLOW}   Configure em $PROD_ENV_PATH${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Todas as variáveis configuradas${NC}"
echo ""

# Copy project files to production directory
echo -e "${BLUE}📦 Copiando arquivos do projeto...${NC}"
cp -r "$(dirname "$0")"/* "$PROD_DIR/"

# Navigate to production directory
cd "$PROD_DIR/docker"

# Build Docker image
echo -e "${BLUE}🔨 Building Docker image...${NC}"
docker compose build --quiet

# Run pipeline
echo -e "${BLUE}🚀 Executando pipeline...${NC}"
docker compose run --rm etl-processor

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}✅ Deployment concluído com sucesso!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo -e "${YELLOW}📊 Logs: $PROD_DIR/logs/${NC}"
echo -e "${YELLOW}📁 Processados: $PROD_DIR/docker/data/processed/${NC}"
