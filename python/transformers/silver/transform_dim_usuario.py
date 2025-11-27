"""
Módulo: transform_dim_usuario.py
Descrição: Transformador para a dimensão de usuários (dim_usuario) com SCD Type 2.
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from transformers.base_transformer import BaseSilverTransformer
from utils.db_connection import get_db_connection


class TransformDimUsuario(BaseSilverTransformer):
    """
    Transformador para a dimensão de usuários, implementando SCD Type 2 e hierarquia.
    """

    def __init__(self):
        """Inicializa o transformador com seus parâmetros específicos."""
        super().__init__(
            script_name='transform_dim_usuario.py',
            tabela_origem='bronze.usuarios',
            tabela_destino='silver.dim_usuario',
            tipo_carga='scd2',
            chave_natural='usuario_nk'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        """Extrai dados de usuários da camada Bronze."""
        return pd.read_sql("SELECT * FROM bronze.usuarios", conn)

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica transformações de negócio aos dados de usuários."""

        # 1. Chave Natural e Campos Básicos
        df['usuario_nk'] = df['email'].fillna(df['nome'])
        df['nome_completo'] = df['nome']
        df['status_usuario'] = 'ATIVO'
        df['data_cadastro'] = datetime.now().date()
        df['data_ultimo_acesso'] = None

        # 2. Renomear colunas Bronze → Silver
        df = df.rename(columns={
            'canal_1': 'canal_principal',
            'canal_2': 'canal_secundario'
        })

        # 3. Derivar tipo_canal e nivel_hierarquia
        df['tipo_canal'] = df['canal_principal']  # Simplificado
        df['nivel_hierarquia'] = df['gestor'].apply(lambda x: 1 if x == 'Sim' else 2 if x else 3)

        # 4. Resolver Hierarquia de Gestores (gestor_sk)
        try:
            with get_db_connection() as conn:
                # Busca os gestores ativos na própria dimensão
                query_gestores = """
                SELECT usuario_sk AS gestor_sk, email
                FROM silver.dim_usuario
                WHERE flag_ativo = TRUE
                """
                df_gestores = pd.read_sql(query_gestores, conn)

                if not df_gestores.empty:
                    # Junta os dados da bronze com os gestores da silver
                    df = df.merge(
                        df_gestores.rename(columns={'email': 'email_lider'}),
                        on='email_lider',
                        how='left'
                    )
                else:
                    df['gestor_sk'] = None
        except Exception:
            self.logger.warning("[SILVER][AVISO] Não foi possível buscar gestores. Assumindo primeira carga.")
            df['gestor_sk'] = None

        # 5. Colunas para o Hash do SCD2
        colunas_hash = [
            'usuario_nk', 'nome_completo', 'email', 'nome_empresa', 'area',
            'senioridade', 'gestor', 'email_lider', 'tipo_canal', 'canal_principal',
            'canal_secundario', 'status_usuario'
        ]

        df['hash_registro'] = df.apply(lambda row: self.calcular_hash_registro(row, colunas_hash), axis=1)

        # 6. Selecionar APENAS colunas que existem na Silver
        colunas_silver = [
            'usuario_nk', 'nome_completo', 'email', 'nome_empresa', 'area',
            'senioridade', 'gestor', 'email_lider', 'tipo_canal', 'canal_principal',
            'canal_secundario', 'status_usuario', 'data_cadastro', 'data_ultimo_acesso',
            'gestor_sk', 'nivel_hierarquia', 'hash_registro'
        ]

        return df[colunas_silver]

    def validar_qualidade(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        """Valida a qualidade dos dados transformados."""
        erros = []
        if df['usuario_nk'].isnull().any():
            erros.append("Campo 'usuario_nk' (chave natural) não pode ser nulo.")
        
        if df['nome_completo'].isnull().any():
            erros.append("Campo 'nome_completo' não pode ser nulo.")

        if df['email'].isnull().any():
            erros.append("Campo 'email' não pode ser nulo.")

        return len(erros) == 0, erros


if __name__ == '__main__':
    # Permite a execução do script diretamente
    sys.exit(TransformDimUsuario().executar())