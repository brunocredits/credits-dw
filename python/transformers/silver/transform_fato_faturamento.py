#!/usr/bin/env python3
"""
Módulo: transform_fato_faturamento.py
Descrição: Transformador para a tabela de fatos de faturamento (fato_faturamento).
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from transformers.base_transformer import BaseSilverTransformer
from utils.db_connection import get_db_connection


class TransformFatoFaturamento(BaseSilverTransformer):
    """
    Transformador para a tabela de fatos de faturamento.
    """

    def __init__(self):
        """Inicializa o transformador com seus parâmetros específicos."""
        super().__init__(
            script_name='transform_fato_faturamento.py',
            tabela_origem='bronze.faturamentos',
            tabela_destino='silver.fato_faturamento',
            tipo_carga='full'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        """Extrai dados de faturamento da camada Bronze."""
        return pd.read_sql("SELECT * FROM bronze.faturamentos", conn)

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica transformações de negócio aos dados de faturamento."""

        # 1. Limpeza e Padronização de Chaves de Negócio
        df['cnpj_cliente_limpo'] = df['cnpj_cliente'].str.replace(r'\D', '', regex=True)
        df['email_usuario_limpo'] = df['email_usuario'].str.strip().str.lower()
        df['data_referencia'] = pd.to_datetime(df['data'], errors='coerce').dt.date

        # 2. Lookup de Chaves Estrangeiras (SKs) nas Dimensões
        with get_db_connection() as conn:
            # dim_data
            dim_data = pd.read_sql(
                "SELECT data_sk, data_completa FROM silver.dim_data", conn
            )

            # dim_cliente (ativos)
            dim_cliente = pd.read_sql("""
                SELECT cliente_sk, cnpj_cpf_nk
                FROM silver.dim_cliente
                WHERE flag_ativo = TRUE
            """, conn)

            # dim_usuario (ativos)
            dim_usuario = pd.read_sql("""
                SELECT usuario_sk, email
                FROM silver.dim_usuario
                WHERE flag_ativo = TRUE
            """, conn)

            # dim_canal (ainda não implementado, usar NULL)
            # Quando dim_canal for populada, fazer lookup como nas outras dimensões
            df['canal_sk'] = None  # NULL até dim_canal ser implementada

        df = df.merge(dim_data, left_on='data_referencia', right_on='data_completa', how='left')
        df = df.merge(dim_cliente, left_on='cnpj_cliente_limpo', right_on='cnpj_cpf_nk', how='left')
        df = df.merge(dim_usuario, left_on='email_usuario_limpo', right_on='email', how='left')

        # 3. Cálculo de Medidas Derivadas e Atributos da Fato
        df['valor_bruto'] = pd.to_numeric(df['receita'], errors='coerce')
        df['valor_desconto'] = 0
        df['valor_liquido'] = df['valor_bruto'] - df['valor_desconto']
        df['valor_imposto'] = df['valor_bruto'] * 0.15
        df['valor_comissao'] = df['valor_bruto'] * 0.05
        df['quantidade'] = 1
        df['numero_parcelas'] = 1
        df['numero_documento'] = None
        df['tipo_documento'] = 'FATURA'
        df['moeda'] = df['moeda'].fillna('BRL') # Default
        df['forma_pagamento'] = 'BOLETO'
        df['status_pagamento'] = 'PAGO'
        df['data_vencimento'] = df['data_referencia']
        df['data_pagamento'] = df['data_referencia']
        df['origem_dado'] = 'CSV'
        df['data_processamento'] = datetime.now()

        # 4. Hash para Idempotência (garante que a mesma transação não seja inserida 2x)
        colunas_hash = [
            'cnpj_cliente_limpo', 'email_usuario_limpo', 'data_referencia',
            'valor_bruto', 'moeda'
        ]
        df['hash_transacao'] = df.apply(
            lambda row: self.calcular_hash_registro(row, colunas_hash), axis=1
        )

        # 5. Selecionar e reordenar colunas finais
        colunas_finais = [
            'cliente_sk', 'usuario_sk', 'data_sk', 'canal_sk',
            'valor_bruto', 'valor_desconto', 'valor_liquido', 'valor_imposto', 'valor_comissao',
            'quantidade', 'numero_parcelas', 'numero_documento', 'tipo_documento', 'moeda',
            'forma_pagamento', 'status_pagamento', 'data_vencimento', 'data_pagamento',
            'origem_dado', 'data_processamento', 'hash_transacao'
        ]

        return df[colunas_finais]


    def validar_qualidade(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        """Valida a qualidade dos dados transformados antes da carga."""
        erros = []
        # === VALIDAÇÕES CRÍTICAS (BLOQUEIAM A EXECUÇÃO) ===

        # 1. sk_data: Obrigatório (uma fato sem data não faz sentido)
        if df['data_sk'].isnull().any():
            count = df['data_sk'].isnull().sum()
            erros.append(f"ERRO CRÍTICO: {count} registros sem data_sk. Verifique se a dim_data está populada para as datas do faturamento.")

        # 2. cliente_sk: Obrigatório para a integridade do modelo
        if df['cliente_sk'].isnull().any():
            count = df['cliente_sk'].isnull().sum()
            erros.append(f"ERRO CRÍTICO: {count} registros sem cliente_sk. CNPJs não encontrados na dim_cliente.")
            # Logar detalhes dos CNPJs órfãos para debugging
            cnpjs_orfaos = df[df['cliente_sk'].isnull()]['cnpj_cliente_limpo'].unique()
            self.logger.error(f"[SILVER][ERRO] CNPJs órfãos: {list(cnpjs_orfaos)[:10]}")

        # 3. usuario_sk: Obrigatório para rastreabilidade
        if df['usuario_sk'].isnull().any():
            count = df['usuario_sk'].isnull().sum()
            erros.append(f"ERRO CRÍTICO: {count} registros sem usuario_sk. Emails não encontrados na dim_usuario.")
            # Logar detalhes dos emails órfãos
            emails_orfaos = df[df['usuario_sk'].isnull()]['email_usuario_limpo'].unique()
            self.logger.error(f"[SILVER][ERRO] Emails órfãos: {list(emails_orfaos)[:10]}")

        # 4. valor_bruto: Obrigatório e deve ser > 0
        if df['valor_bruto'].isnull().any():
            count = df['valor_bruto'].isnull().sum()
            erros.append(f"ERRO CRÍTICO: {count} valores brutos nulos ou inválidos.")
        
        if (df['valor_bruto'] <= 0).any():
            count = (df['valor_bruto'] <= 0).sum()
            erros.append(f"ERRO CRÍTICO: {count} valores brutos <= 0. Ajustar regras de negócio para valores negativos.")


        return len(erros) == 0, erros


if __name__ == '__main__':
    # Permite a execução do script diretamente
    sys.exit(TransformFatoFaturamento().executar())