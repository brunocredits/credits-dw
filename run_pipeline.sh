#!/bin/bash
# ============================================================================
# Credits DW - Pipeline Runner
# Executa o pipeline de ingestão Bronze via Docker
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}🚀 Credits DW - Pipeline de Ingestão Bronze${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo -e "${RED}❌ Arquivo .env não encontrado!${NC}"
    echo -e "${YELLOW}📝 Copie o .env.example e configure as variáveis${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Arquivo .env encontrado${NC}"
echo -e "${BLUE}📝 Usando credenciais:${NC}"
echo -e "${YELLOW}   DB: $(grep '^DB_NAME=' .env | cut -d'=' -f2)${NC}"
echo -e "${YELLOW}   User: $(grep '^DB_USER=' .env | cut -d'=' -f2)${NC}"
echo -e "${YELLOW}   Host: $(grep '^DB_HOST=' .env | cut -d'=' -f2)${NC}"
echo ""

# Load .env variables and export them
set -a  # automatically export all variables
source .env
set +a

# Navigate to docker directory
cd docker

# Build
echo -e "${BLUE}🔨 Building Docker image...${NC}"
docker compose build --quiet

echo ""
echo -e "${BLUE}🚀 Running pipeline...${NC}"
docker compose run --rm etl-processor

echo ""
echo -e "${GREEN}✅ Pipeline concluído!${NC}"
echo -e "${YELLOW}📊 Verifique os logs em: logs/${NC}"
echo -e "${YELLOW}📁 Arquivos processados em: docker/data/processed/${NC}"

