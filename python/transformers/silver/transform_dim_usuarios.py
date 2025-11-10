import pandas as pd
from datetime import datetime
from transformers.base_transformer import BaseSilverTransformer

class TransformDimUsuarios(BaseSilverTransformer):
    def __init__(self):
        super().__init__(
            script_name='transform_dim_usuarios.py',
            tabela_origem='bronze.usuarios',
            tabela_destino='silver.dim_usuarios',
            tipo_carga='full',  # Mudado de scd2 para full
            chave_natural='nk_usuario'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        return pd.read_sql("SELECT * FROM bronze.usuarios", conn)

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        df['nk_usuario'] = df['email'].fillna(df['Nome'])
        df['nome_completo'] = df['Nome']
        df['tipo_canal'] = 'DIRETO'
        df['canal_principal'] = df['canal_1']
        df['canal_secundario'] = df['canal_2']
        df['status_usuario'] = 'ATIVO'
        df['data_cadastro'] = datetime.now().date()
        df['data_ultimo_acesso'] = None
        df['sk_gestor'] = None
        df['nivel_hierarquia'] = 1

        df.rename(columns={
            'nome_empresa': 'nome_empresa',
            'area': 'area',
            'senioridade': 'senioridade',
            'gestor': 'gestor',
            'email': 'email',
            'email_lider': 'email_lider'
        }, inplace=True)

        cols = ['nk_usuario', 'nome_completo', 'email', 'nome_empresa', 'area', 'senioridade',
                'gestor', 'email_lider', 'tipo_canal', 'canal_principal', 'canal_secundario',
                'status_usuario', 'data_cadastro', 'data_ultimo_acesso', 'sk_gestor', 'nivel_hierarquia']

        df['data_inicio'] = datetime.now().date()
        df['data_fim'] = None
        df['flag_ativo'] = True
        df['data_carga'] = datetime.now()

        return df[cols + ['data_inicio', 'data_fim', 'flag_ativo', 'data_carga']]

    def validar_qualidade(self, df: pd.DataFrame):
        erros = []
        if df['nk_usuario'].isnull().any():
            erros.append("Usuarios sem identificador")
        if df['nome_completo'].isnull().any():
            erros.append("Usuarios sem nome")
        return len(erros) == 0, erros

if __name__ == '__main__':
    TransformDimUsuarios().executar()
