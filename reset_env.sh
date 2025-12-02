#!/bin/bash
# Script para resetar o ambiente (Banco e Arquivos)

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}🧹 Credits DW - Reset de Ambiente${NC}"
echo -e "${BLUE}============================================================${NC}"

# Check .env
if [ ! -f .env ]; then
    echo -e "${RED}❌ Arquivo .env não encontrado!${NC}"
    exit 1
fi

# Load .env
export $(cat .env | grep -v '^#' | xargs)

echo -e "${YELLOW}⚠️  ATENÇÃO: Isso vai apagar todos os dados das tabelas Bronze (exceto datas) e limpar os arquivos processados.${NC}"
echo -e "${YELLOW}   Tem certeza? (s/N)${NC}"
read -r response
if [[ ! "$response" =~ ^([sS][sS]|[sS])$ ]]; then
    echo -e "${RED}Cancelado.${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}1. Limpar Arquivos Processados e Logs...${NC}"
# Use docker to bypass permission issues
docker run --rm -v $(pwd)/docker/data:/data alpine sh -c "rm -rf /data/processed/*"
docker run --rm -v $(pwd)/logs:/logs alpine sh -c "rm -rf /logs/*"
echo -e "${GREEN}   ✅ Arquivos limpos.${NC}"

echo ""
echo -e "${BLUE}2. Truncar Tabelas do Banco...${NC}"
export PGPASSWORD=$DB_PASSWORD
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << 'EOF'
TRUNCATE TABLE bronze.faturamento RESTART IDENTITY CASCADE;
TRUNCATE TABLE bronze.base_oficial RESTART IDENTITY CASCADE;
TRUNCATE TABLE bronze.usuarios RESTART IDENTITY CASCADE;
TRUNCATE TABLE auditoria.log_rejeicao RESTART IDENTITY CASCADE;
TRUNCATE TABLE auditoria.historico_execucao RESTART IDENTITY CASCADE;
EOF
echo -e "${GREEN}   ✅ Tabelas truncadas (bronze.data preservada).${NC}"

echo ""
echo -e "${GREEN}✨ Ambiente 100% Resetado! Pronto para nova carga.${NC}"
