import pandas as pd
from datetime import datetime
from transformers.base_transformer import BaseSilverTransformer

class TransformDimClientes(BaseSilverTransformer):
    def __init__(self):
        super().__init__(
            script_name='transform_dim_clientes.py',
            tabela_origem='bronze.contas_base_oficial',
            tabela_destino='silver.dim_clientes',
            tipo_carga='scd2',
            chave_natural='nk_cnpj_cpf'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        return pd.read_sql("SELECT * FROM bronze.contas_base_oficial", conn)

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        df['nk_cnpj_cpf'] = df['cnpj_cpf'].str.replace(r'\D', '', regex=True)
        df['tipo_pessoa'] = df['nk_cnpj_cpf'].apply(lambda x: 'PJ' if len(str(x)) > 11 else 'PF')
        df['cnpj_cpf_formatado'] = df['cnpj_cpf']
        df['porte_empresa'] = 'MEDIO'

        # Calcular tempo como cliente em dias (converter para int)
        df['data_criacao_dt'] = pd.to_datetime(df['data_criacao'])
        df['tempo_cliente_dias'] = (datetime.now() - df['data_criacao_dt']).dt.days.fillna(0).astype(int)
        df.drop('data_criacao_dt', axis=1, inplace=True)

        df['categoria_risco'] = 'BAIXO'

        df.rename(columns={
            'status': 'status',
            'status_qualificação_da_conta': 'status_qualificacao',
            'razao_social': 'razao_social',
            'grupo': 'grupo',
            'responsavel_conta': 'responsavel_conta',
            'financeiro': 'email_financeiro',
            'corte': 'corte',
            'faixa': 'faixa'
        }, inplace=True)

        cols = ['nk_cnpj_cpf', 'razao_social', 'tipo_pessoa', 'status', 'status_qualificacao',
                'grupo', 'responsavel_conta', 'email_financeiro', 'corte', 'faixa',
                'cnpj_cpf_formatado', 'porte_empresa', 'tempo_cliente_dias', 'categoria_risco']

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
        erros = []
        if df['nk_cnpj_cpf'].isnull().any():
            erros.append("CNPJs/CPFs nulos encontrados")
        if df.duplicated('nk_cnpj_cpf').any():
            erros.append("CNPJs/CPFs duplicados encontrados")
        return len(erros) == 0, erros

if __name__ == '__main__':
    TransformDimClientes().executar()
