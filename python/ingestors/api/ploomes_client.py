#!/usr/bin/env python3
"""
Módulo: ploomes_client.py
Descrição: Cliente para API do Ploomes CRM
Documentação: https://developers.ploomes.com
Versão: 2.0
"""

import os
import sys
import requests
from typing import Dict, List, Optional
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent.parent))

from utils.logger import setup_logger

logger = setup_logger('ploomes_client')


class PloomesClient:
    """
    Cliente para interagir com a API do Ploomes.
    """

    def __init__(self, api_key: Optional[str] = None, api_url: Optional[str] = None):
        """
        Inicializa o cliente Ploomes.

        Args:
            api_key: Chave de API do Ploomes (se None, pega do .env)
            api_url: URL base da API (se None, pega do .env)
        """
        self.api_key = api_key or os.getenv('PLOOMES_API_KEY')
        self.api_url = (api_url or os.getenv('PLOOMES_API_URL', 'https://api2.ploomes.com')).rstrip('/')

        if not self.api_key:
            raise ValueError("PLOOMES_API_KEY não configurada")

        self.headers = {
            'User-Key': self.api_key,
            'Content-Type': 'application/json'
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        logger.info(f"Cliente Ploomes inicializado: {self.api_url}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Faz requisição GET com retry automático.

        Args:
            endpoint: Endpoint da API (ex: '/Contacts')
            params: Parâmetros query string

        Returns:
            JSON response como dicionário
        """
        url = f"{self.api_url}{endpoint}"

        logger.debug(f"GET {url} - Params: {params}")

        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response.json()

    def get_contacts(self, page: int = 1, page_size: int = 100) -> Dict:
        """
        Busca contatos do Ploomes.

        Args:
            page: Número da página (começa em 1)
            page_size: Quantidade de registros por página (max 100)

        Returns:
            Dict com 'value' (lista de contatos) e metadados de paginação
        """
        params = {
            '$top': page_size,
            '$skip': (page - 1) * page_size,
            '$count': 'true'
        }

        logger.info(f"Buscando contatos - Página {page}, tamanho {page_size}")
        return self._get('/Contacts', params=params)

    def get_all_contacts(self, max_records: Optional[int] = None) -> List[Dict]:
        """
        Busca TODOS os contatos do Ploomes com paginação automática.

        Args:
            max_records: Limite máximo de registros (None = todos)

        Returns:
            Lista completa de contatos
        """
        all_contacts = []
        page = 1
        page_size = 100

        logger.info("Iniciando busca de todos os contatos...")

        while True:
            result = self.get_contacts(page=page, page_size=page_size)

            contacts = result.get('value', [])
            if not contacts:
                break

            all_contacts.extend(contacts)
            logger.info(f"Coletados {len(all_contacts)} contatos até agora...")

            # Verificar se atingiu o limite
            if max_records and len(all_contacts) >= max_records:
                all_contacts = all_contacts[:max_records]
                break

            # Verificar se há mais páginas
            total_count = result.get('@odata.count')
            if total_count and len(all_contacts) >= total_count:
                break

            page += 1

        logger.info(f"Busca concluída. Total de contatos: {len(all_contacts)}")
        return all_contacts

    def get_deals(self, page: int = 1, page_size: int = 100) -> Dict:
        """
        Busca deals (oportunidades) do Ploomes.

        Args:
            page: Número da página
            page_size: Quantidade de registros por página

        Returns:
            Dict com 'value' (lista de deals) e metadados
        """
        params = {
            '$top': page_size,
            '$skip': (page - 1) * page_size,
            '$count': 'true'
        }

        logger.info(f"Buscando deals - Página {page}, tamanho {page_size}")
        return self._get('/Deals', params=params)

    def get_all_deals(self, max_records: Optional[int] = None) -> List[Dict]:
        """
        Busca TODOS os deals do Ploomes com paginação automática.

        Args:
            max_records: Limite máximo de registros

        Returns:
            Lista completa de deals
        """
        all_deals = []
        page = 1
        page_size = 100

        logger.info("Iniciando busca de todos os deals...")

        while True:
            result = self.get_deals(page=page, page_size=page_size)

            deals = result.get('value', [])
            if not deals:
                break

            all_deals.extend(deals)
            logger.info(f"Coletados {len(all_deals)} deals até agora...")

            if max_records and len(all_deals) >= max_records:
                all_deals = all_deals[:max_records]
                break

            total_count = result.get('@odata.count')
            if total_count and len(all_deals) >= total_count:
                break

            page += 1

        logger.info(f"Busca concluída. Total de deals: {len(all_deals)}")
        return all_deals

    def get_companies(self, page: int = 1, page_size: int = 100) -> Dict:
        """
        Busca empresas do Ploomes.

        Args:
            page: Número da página
            page_size: Quantidade de registros por página

        Returns:
            Dict com 'value' (lista de empresas) e metadados
        """
        params = {
            '$top': page_size,
            '$skip': (page - 1) * page_size,
            '$count': 'true'
        }

        logger.info(f"Buscando empresas - Página {page}, tamanho {page_size}")
        return self._get('/Companies', params=params)

    def get_all_companies(self, max_records: Optional[int] = None) -> List[Dict]:
        """
        Busca TODAS as empresas do Ploomes com paginação automática.

        Args:
            max_records: Limite máximo de registros

        Returns:
            Lista completa de empresas
        """
        all_companies = []
        page = 1
        page_size = 100

        logger.info("Iniciando busca de todas as empresas...")

        while True:
            result = self.get_companies(page=page, page_size=page_size)

            companies = result.get('value', [])
            if not companies:
                break

            all_companies.extend(companies)
            logger.info(f"Coletados {len(all_companies)} empresas até agora...")

            if max_records and len(all_companies) >= max_records:
                all_companies = all_companies[:max_records]
                break

            total_count = result.get('@odata.count')
            if total_count and len(all_companies) >= total_count:
                break

            page += 1

        logger.info(f"Busca concluída. Total de empresas: {len(all_companies)}")
        return all_companies

    def close(self):
        """Fecha a sessão HTTP"""
        self.session.close()
        logger.info("Cliente Ploomes encerrado")


# Exemplo de uso
if __name__ == '__main__':
    # Teste do cliente
    client = PloomesClient()

    try:
        # Testar conexão buscando primeira página de contatos
        result = client.get_contacts(page=1, page_size=10)
        print(f"✓ Conexão OK - Total de contatos: {result.get('@odata.count', 'N/A')}")
        print(f"✓ Primeira página retornou {len(result.get('value', []))} registros")

    except Exception as e:
        print(f"✗ Erro ao conectar: {str(e)}")

    finally:
        client.close()
