#!/usr/bin/env python3
"""
Módulo: transform_dim_clientes.py
Descrição: Transformador para dimensão de clientes com SCD Type 2
Versão: 2.0
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
from datetime import datetime
from transformers.base_transformer import BaseSilverTransformer


class TransformDimClientes(BaseSilverTransformer):
    """
    Transformador para dimensão de clientes.

    Aplica transformações:
    - Padronização de CNPJ/CPF
    - Classificação de tipo de pessoa (PJ/PF)
    - Cálculo de tempo como cliente
    - Categorização de risco
    - SCD Type 2 para rastreamento de mudanças
    """

    def __init__(self):
        super().__init__(
            script_name='transform_dim_clientes.py',
            tabela_origem='bronze.contas_base_oficial',
            tabela_destino='silver.dim_clientes',
            tipo_carga='scd2',
            chave_natural='nk_cnpj_cpf'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        """Extrai dados de contas da Bronze"""
        return pd.read_sql("SELECT * FROM bronze.contas_base_oficial", conn)

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformações de negócio aos dados de clientes.

        Transformações:
        - Padroniza CNPJ/CPF (remove pontuação)
        - Determina tipo de pessoa (PJ/PF) baseado no tamanho
        - Calcula tempo como cliente
        - Categoriza risco (implementação simplificada)
        - Adiciona campos SCD Type 2
        """
        # Padronizar CNPJ/CPF (chave natural)
        df['nk_cnpj_cpf'] = df['cnpj_cpf'].str.replace(r'\D', '', regex=True)
        df['tipo_pessoa'] = df['nk_cnpj_cpf'].apply(lambda x: 'PJ' if len(str(x)) > 11 else 'PF')
        df['cnpj_cpf_formatado'] = df['cnpj_cpf']

        # TODO: Implementar lógica real de porte_empresa
        df['porte_empresa'] = 'MEDIO'

        # Calcular tempo como cliente
        df['data_criacao_dt'] = pd.to_datetime(df['data_criacao'])
        df['tempo_cliente_dias'] = (datetime.now() - df['data_criacao_dt']).dt.days.fillna(0).astype(int)
        df.drop('data_criacao_dt', axis=1, inplace=True)

        # TODO: Implementar lógica real de categoria_risco
        df['categoria_risco'] = 'BAIXO'

        # Renomear colunas
        df.rename(columns={
            'status_qualificação_da_conta': 'status_qualificacao',
            'financeiro': 'email_financeiro'
        }, inplace=True)

        # Colunas para hash SCD2
        cols = ['nk_cnpj_cpf', 'razao_social', 'tipo_pessoa', 'status', 'status_qualificacao',
                'grupo', 'responsavel_conta', 'email_financeiro', 'corte', 'faixa',
                'cnpj_cpf_formatado', 'porte_empresa', 'tempo_cliente_dias', 'categoria_risco']

        # Campos SCD Type 2
        df['hash_registro'] = df.apply(lambda r: self.calcular_hash_registro(r, cols), axis=1)
        df['data_inicio'] = datetime.now().date()
        df['data_fim'] = None
        df['flag_ativo'] = True
        df['versao'] = 1
        df['motivo_mudanca'] = 'Carga inicial'
        df['data_carga'] = datetime.now()
        df['data_criacao'] = pd.to_datetime(df['data_criacao']).dt.date

        return df[cols + ['hash_registro', 'data_inicio', 'data_fim', 'flag_ativo', 'versao', 'motivo_mudanca', 'data_carga', 'data_criacao']]

    def validar_qualidade(self, df: pd.DataFrame):
        """
        Valida qualidade dos dados de clientes.

        Validações:
        - CNPJ/CPF não pode ser nulo
        - Não pode haver CNPJs/CPFs duplicados
        - Razão social obrigatória para PJ
        """
        erros = []

        if df['nk_cnpj_cpf'].isnull().any():
            erros.append("CNPJs/CPFs nulos encontrados")

        if df.duplicated('nk_cnpj_cpf').any():
            count = df.duplicated('nk_cnpj_cpf').sum()
            erros.append(f"{count} CNPJs/CPFs duplicados encontrados")

        # Validar razão social para PJ
        pj_sem_razao = df[(df['tipo_pessoa'] == 'PJ') & (df['razao_social'].isnull())]
        if not pj_sem_razao.empty:
            erros.append(f"{len(pj_sem_razao)} empresas (PJ) sem razão social")

        return len(erros) == 0, erros


if __name__ == '__main__':
    import sys
    sys.exit(TransformDimClientes().executar())
