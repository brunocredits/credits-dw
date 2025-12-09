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

echo -e "${YELLOW}⚠️  ATENÇÃO: Isso vai apagar todos os dados das tabelas Bronze (exceto datas) e limpar os arquivos processados.${NC}"
echo -e "${YELLOW}   Tem certeza? (s/N)${NC}"
read -r response
if [[ ! "$response" =~ ^([sS][sS]|[sS])$ ]]; then
    echo -e "${RED}Cancelado.${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}1. Limpar Arquivos Processados e Logs...${NC}"
# Use docker to bypass permission issues on Linux/Mac
docker run --rm -v $(pwd)/docker/data:/data alpine sh -c "rm -rf /data/processed/*"
docker run --rm -v $(pwd)/logs:/logs alpine sh -c "rm -rf /logs/*"
echo -e "${GREEN}   ✅ Arquivos limpos.${NC}"

echo ""
echo -e "${BLUE}2. Truncar Tabelas do Banco...${NC}"
# Executa o script Python dentro do container (não depende de psql local)
cd docker
docker compose build --quiet
docker compose run --rm etl-processor python python/scripts/truncate_tables.py
cd ..

echo ""
echo -e "${GREEN}✨ Ambiente 100% Resetado! Pronto para nova carga.${NC}"