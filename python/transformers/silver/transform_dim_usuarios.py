"""
Módulo: transform_dim_usuarios.py
Descrição: Transformador para dimensão de usuários com SCD Type 2
Versão: 2.0
"""

import pandas as pd
from datetime import datetime
from transformers.base_transformer import BaseSilverTransformer


class TransformDimUsuarios(BaseSilverTransformer):
    """
    Transformador para dimensão de usuários.

    Aplica transformações:
    - Criação de chave natural (email ou nome)
    - Resolução de hierarquia de gestores
    - Extração de canais principais e secundários
    - SCD Type 2 para rastreamento de mudanças
    """

    def __init__(self):
        super().__init__(
            script_name='transform_dim_usuarios.py',
            tabela_origem='bronze.usuarios',
            tabela_destino='silver.dim_usuarios',
            tipo_carga='scd2',
            chave_natural='nk_usuario'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        """Extrai dados de usuários da Bronze"""
        return pd.read_sql("SELECT * FROM bronze.usuarios", conn)

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformações de negócio aos dados de usuários.

        Transformações:
        - Cria chave natural (email ou nome)
        - Resolve hierarquia de gestores (email_lider -> sk_gestor)
        - Extrai canais principal e secundário
        - Adiciona campos SCD Type 2
        """
        from utils.db_connection import get_db_connection

        # Chave natural: preferir email, usar Nome se não tiver
        df['nk_usuario'] = df['email'].fillna(df['Nome'])
        df['nome_completo'] = df['Nome']

        # TODO: Implementar lógica real de tipo_canal baseado em regras de negócio
        df['tipo_canal'] = 'DIRETO'
        df['canal_principal'] = df['canal_1']
        df['canal_secundario'] = df['canal_2']
        df['status_usuario'] = 'ATIVO'
        df['data_cadastro'] = datetime.now().date()
        df['data_ultimo_acesso'] = None

        # Resolver hierarquia de gestores
        try:
            with get_db_connection() as conn:
                query_gestores = """
                SELECT sk_usuario, email
                FROM silver.dim_usuarios
                WHERE flag_ativo = TRUE
                """
                df_gestores = pd.read_sql(query_gestores, conn)

                if not df_gestores.empty:
                    df = df.merge(
                        df_gestores.rename(columns={'email': 'email_lider', 'sk_usuario': 'sk_gestor'}),
                        on='email_lider',
                        how='left'
                    )
                else:
                    df['sk_gestor'] = None
        except Exception:
            # Primeira carga
            df['sk_gestor'] = None

        # Nível hierárquico
        df['nivel_hierarquia'] = df['sk_gestor'].apply(lambda x: 1 if pd.isna(x) else 2)

        # Colunas para hash SCD2
        cols = ['nk_usuario', 'nome_completo', 'email', 'nome_empresa', 'area', 'senioridade',
                'gestor', 'email_lider', 'tipo_canal', 'canal_principal', 'canal_secundario',
                'status_usuario', 'data_cadastro', 'data_ultimo_acesso', 'sk_gestor', 'nivel_hierarquia']

        # Campos SCD Type 2
        df['hash_registro'] = df.apply(lambda r: self.calcular_hash_registro(r, cols), axis=1)
        df['data_inicio'] = datetime.now().date()
        df['data_fim'] = None
        df['flag_ativo'] = True
        df['versao'] = 1
        df['motivo_mudanca'] = 'Carga inicial'
        df['data_carga'] = datetime.now()

        return df[cols + ['hash_registro', 'data_inicio', 'data_fim', 'flag_ativo', 'versao', 'motivo_mudanca', 'data_carga']]

    def validar_qualidade(self, df: pd.DataFrame):
        """
        Valida qualidade dos dados de usuários.

        Validações:
        - Chave natural (nk_usuario) obrigatória
        - Nome completo obrigatório
        - Email obrigatório
        """
        erros = []

        if df['nk_usuario'].isnull().any():
            count = df['nk_usuario'].isnull().sum()
            erros.append(f"{count} usuários sem identificador")

        if df['nome_completo'].isnull().any():
            count = df['nome_completo'].isnull().sum()
            erros.append(f"{count} usuários sem nome")

        if df['email'].isnull().any():
            count = df['email'].isnull().sum()
            erros.append(f"{count} usuários sem email")

        return len(erros) == 0, erros


if __name__ == '__main__':
    import sys
    sys.exit(TransformDimUsuarios().executar())
