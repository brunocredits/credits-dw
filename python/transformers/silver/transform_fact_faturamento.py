#!/usr/bin/env python3
"""Transformador para fact_faturamento"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
from datetime import datetime
from transformers.base_transformer import BaseSilverTransformer
from utils.db_connection import get_db_connection

class TransformFactFaturamento(BaseSilverTransformer):
    def __init__(self):
        super().__init__(
            script_name='transform_fact_faturamento.py',
            tabela_origem='bronze.faturamento',
            tabela_destino='silver.fact_faturamento',
            tipo_carga='full'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        return pd.read_sql("SELECT * FROM bronze.faturamento", conn)

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        # Limpar CNPJ (remover pontuação)
        df['cnpj_limpo'] = df['cnpj_cliente'].str.replace(r'\D', '', regex=True)
        df['email_limpo'] = df['email_usuario'].str.strip().str.lower()
        df['data_ref'] = pd.to_datetime(df['data']).dt.date

        # Obter dimensões para lookup de FKs
        with get_db_connection() as conn:
            # dim_tempo
            dim_tempo = pd.read_sql(
                "SELECT sk_data, data_completa FROM silver.dim_tempo", conn
            )

            # dim_clientes ativos (lookup por CNPJ limpo)
            dim_clientes = pd.read_sql("""
                SELECT sk_cliente, nk_cnpj_cpf
                FROM silver.dim_clientes
                WHERE flag_ativo = TRUE
            """, conn)

            # dim_usuarios ativos (lookup por email)
            dim_usuarios = pd.read_sql("""
                SELECT sk_usuario, email
                FROM silver.dim_usuarios
                WHERE flag_ativo = TRUE
            """, conn)

            # dim_canal (usar primeiro canal como padrão)
            dim_canal = pd.read_sql(
                "SELECT sk_canal FROM silver.dim_canal ORDER BY sk_canal LIMIT 1", conn
            )

        # JOIN com dimensões
        df = df.merge(dim_tempo, left_on='data_ref', right_on='data_completa', how='left')

        df = df.merge(
            dim_clientes,
            left_on='cnpj_limpo',
            right_on='nk_cnpj_cpf',
            how='left'
        )

        df = df.merge(
            dim_usuarios.rename(columns={'email': 'email_limpo'}),
            on='email_limpo',
            how='left'
        )

        # sk_canal padrão
        if not dim_canal.empty:
            df['sk_canal'] = dim_canal['sk_canal'].iloc[0]
        else:
            df['sk_canal'] = None

        # Calcular medidas
        df['valor_bruto'] = pd.to_numeric(df['receita'], errors='coerce')
        df['valor_desconto'] = 0
        df['valor_liquido'] = df['valor_bruto']
        df['valor_imposto'] = df['valor_bruto'] * 0.15
        df['valor_comissao'] = df['valor_bruto'] * 0.05
        df['quantidade'] = 1
        df['numero_parcelas'] = 1
        df['numero_documento'] = None
        df['tipo_documento'] = 'FATURA'
        df['forma_pagamento'] = 'BOLETO'
        df['status_pagamento'] = 'PAGO'
        df['data_vencimento'] = df['data_ref']
        df['data_pagamento'] = df['data_ref']
        df['origem_dado'] = 'CSV'
        df['data_processamento'] = datetime.now()

        # Hash para idempotência
        df['hash_transacao'] = df.apply(
            lambda r: self.calcular_hash_registro(
                r, ['cnpj_limpo', 'email_limpo', 'data_ref', 'valor_bruto', 'moeda']
            ), axis=1
        )

        cols = [
            'sk_cliente', 'sk_usuario', 'sk_data', 'sk_canal',
            'valor_bruto', 'valor_desconto', 'valor_liquido', 'valor_imposto', 'valor_comissao',
            'quantidade', 'numero_parcelas', 'numero_documento', 'tipo_documento', 'moeda',
            'forma_pagamento', 'status_pagamento', 'data_vencimento', 'data_pagamento',
            'origem_dado', 'data_processamento', 'hash_transacao'
        ]

        return df[cols]

    def validar_qualidade(self, df: pd.DataFrame):
        """
        Valida qualidade dos dados da fact_faturamento.

        Validações CRÍTICAS (bloqueiam execução):
        - sk_data obrigatório (fato sem data não faz sentido)
        - sk_cliente obrigatório (fato órfão sem cliente)
        - sk_usuario obrigatório (fato sem responsável)
        - valor_bruto obrigatório e > 0

        Validações INFORMATIVAS (apenas warnings):
        - sk_canal pode ser nulo (será atribuído canal padrão)
        """
        erros = []
        warnings = []

        # === VALIDAÇÕES CRÍTICAS (BLOQUEIAM) ===

        # 1. SK_DATA: Obrigatório
        if df['sk_data'].isnull().any():
            count = df['sk_data'].isnull().sum()
            total = len(df)
            erros.append(f"ERRO CRÍTICO: {count}/{total} registros sem sk_data (datas não encontradas na dim_tempo)")
            self.logger.error(f"❌ {count} registros sem data válida")

        # 2. SK_CLIENTE: Obrigatório para integridade do modelo
        if df['sk_cliente'].isnull().any():
            count = df['sk_cliente'].isnull().sum()
            total = len(df)
            erros.append(f"ERRO CRÍTICO: {count}/{total} registros sem sk_cliente (CNPJs não encontrados na dim_clientes)")
            self.logger.error(f"❌ {count} registros sem cliente válido")

            # Logar detalhes dos CNPJs órfãos para debugging
            if 'cnpj_limpo' in df.columns:
                cnpjs_orfaos = df[df['sk_cliente'].isnull()]['cnpj_limpo'].unique()
                self.logger.error(f"   CNPJs órfãos: {list(cnpjs_orfaos)[:10]}")  # Mostrar até 10

        # 3. SK_USUARIO: Obrigatório para rastreabilidade
        if df['sk_usuario'].isnull().any():
            count = df['sk_usuario'].isnull().sum()
            total = len(df)
            erros.append(f"ERRO CRÍTICO: {count}/{total} registros sem sk_usuario (emails não encontrados na dim_usuarios)")
            self.logger.error(f"❌ {count} registros sem usuário válido")

            # Logar detalhes dos emails órfãos
            if 'email_limpo' in df.columns:
                emails_orfaos = df[df['sk_usuario'].isnull()]['email_limpo'].unique()
                self.logger.error(f"   Emails órfãos: {list(emails_orfaos)[:10]}")

        # 4. VALOR_BRUTO: Obrigatório e deve ser > 0
        if df['valor_bruto'].isnull().any():
            count = df['valor_bruto'].isnull().sum()
            erros.append(f"ERRO CRÍTICO: {count} valores brutos nulos ou inválidos")

        if (df['valor_bruto'] <= 0).any():
            count = (df['valor_bruto'] <= 0).sum()
            erros.append(f"ERRO CRÍTICO: {count} valores brutos <= 0 (valor inválido)")

        # === VALIDAÇÕES INFORMATIVAS (WARNINGS) ===

        # SK_CANAL: Pode ser nulo (será atribuído canal padrão)
        if df['sk_canal'].isnull().any():
            count = df['sk_canal'].isnull().sum()
            total = len(df)
            warnings.append(f"INFO: {count}/{total} registros sem sk_canal (será atribuído canal padrão)")

        # Estatísticas de qualidade
        if not erros:
            self.logger.info("✅ Validações de qualidade aprovadas:")
            self.logger.info(f"   • Total de registros: {len(df):,}")
            self.logger.info(f"   • Registros com FKs válidas: {len(df):,}")
            self.logger.info(f"   • Valor total: R$ {df['valor_bruto'].sum():,.2f}")
            self.logger.info(f"   • Valor médio: R$ {df['valor_bruto'].mean():,.2f}")

        # Logar warnings (não bloqueiam)
        if warnings:
            for warning in warnings:
                self.logger.warning(f"⚠️ {warning}")

        return len(erros) == 0, erros

if __name__ == '__main__':
    import sys
    sys.exit(TransformFactFaturamento().executar())
