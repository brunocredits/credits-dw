import pandas as pd
from datetime import datetime
from transformers.base_transformer import BaseSilverTransformer

class TransformDimUsuarios(BaseSilverTransformer):
    def __init__(self):
        super().__init__(
            script_name='transform_dim_usuarios.py',
            tabela_origem='bronze.usuarios',
            tabela_destino='silver.dim_usuarios',
            tipo_carga='scd2',  # Revertido para SCD Type 2 para rastrear mudanças
            chave_natural='nk_usuario'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        return pd.read_sql("SELECT * FROM bronze.usuarios", conn)

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        from utils.db_connection import get_db_connection

        df['nk_usuario'] = df['email'].fillna(df['Nome'])
        df['nome_completo'] = df['Nome']
        df['tipo_canal'] = 'DIRETO'
        df['canal_principal'] = df['canal_1']
        df['canal_secundario'] = df['canal_2']
        df['status_usuario'] = 'ATIVO'
        df['data_cadastro'] = datetime.now().date()
        df['data_ultimo_acesso'] = None

        # Resolver hierarquia de gestores (email_lider → sk_gestor)
        # Buscar gestores ativos na dimensão
        try:
            with get_db_connection() as conn:
                query_gestores = """
                SELECT sk_usuario, email
                FROM silver.dim_usuarios
                WHERE flag_ativo = TRUE
                """
                df_gestores = pd.read_sql(query_gestores, conn)

                if not df_gestores.empty:
                    # Fazer merge com email_lider para encontrar sk_gestor
                    df = df.merge(
                        df_gestores.rename(columns={'email': 'email_lider', 'sk_usuario': 'sk_gestor'}),
                        on='email_lider',
                        how='left'
                    )
                else:
                    # Primeira carga - nenhum gestor cadastrado ainda
                    df['sk_gestor'] = None
        except Exception as e:
            # Tabela ainda não existe ou está vazia - primeira carga
            df['sk_gestor'] = None

        # Calcular nível hierárquico (1 se não tem gestor, senão será atualizado depois)
        df['nivel_hierarquia'] = df['sk_gestor'].apply(lambda x: 1 if pd.isna(x) else 2)

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

        # Calcular hash para SCD Type 2
        df['hash_registro'] = df.apply(lambda r: self.calcular_hash_registro(r, cols), axis=1)

        df['data_inicio'] = datetime.now().date()
        df['data_fim'] = None
        df['flag_ativo'] = True
        df['versao'] = 1
        df['motivo_mudanca'] = 'Carga inicial'
        df['data_carga'] = datetime.now()

        return df[cols + ['hash_registro', 'data_inicio', 'data_fim', 'flag_ativo', 'versao', 'motivo_mudanca', 'data_carga']]

    def validar_qualidade(self, df: pd.DataFrame):
        erros = []
        if df['nk_usuario'].isnull().any():
            erros.append("Usuarios sem identificador")
        if df['nome_completo'].isnull().any():
            erros.append("Usuarios sem nome")
        return len(erros) == 0, erros

if __name__ == '__main__':
    TransformDimUsuarios().executar()
