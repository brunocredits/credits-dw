"""
Módulo: transform_dim_cliente.py
Descrição: Transformador para a dimensão de clientes (dim_cliente) com SCD Type 2.
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from transformers.base_transformer import BaseSilverTransformer


class TransformDimCliente(BaseSilverTransformer):
    """
    Transformador para a dimensão de clientes, implementando SCD Type 2.
    """

    def __init__(self):
        """Inicializa o transformador com seus parâmetros específicos."""
        super().__init__(
            script_name='transform_dim_cliente.py',
            tabela_origem='bronze.contas',
            tabela_destino='silver.dim_cliente',
            tipo_carga='scd2',
            chave_natural='cnpj_cpf_nk'
        )

    def extrair_bronze(self, conn) -> pd.DataFrame:
        """Extrai dados de contas da camada Bronze."""
        return pd.read_sql("SELECT * FROM bronze.contas", conn)

    def _formatar_cnpj_cpf(self, valor: str) -> str:
        """Formata CNPJ (00.000.000/0000-00) ou CPF (000.000.000-00)."""
        if pd.isna(valor) or not valor:
            return None
        limpo = str(valor).replace(r'\D', '')
        if len(limpo) == 14:  # CNPJ
            return f"{limpo[0:2]}.{limpo[2:5]}.{limpo[5:8]}/{limpo[8:12]}-{limpo[12:14]}"
        elif len(limpo) == 11:  # CPF
            return f"{limpo[0:3]}.{limpo[3:6]}.{limpo[6:9]}-{limpo[9:11]}"
        return valor

    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica transformações de negócio aos dados de clientes."""

        # 1. Padronizar CNPJ/CPF: limpo (só números) + formatado
        df['cnpj_cpf_nk'] = df['cnpj_cpf'].str.replace(r'\D', '', regex=True)
        df['cnpj_cpf_formatado'] = df['cnpj_cpf_nk'].apply(self._formatar_cnpj_cpf)

        # 2. Renomear colunas Bronze → Silver
        df = df.rename(columns={
            'tipo': 'tipo_pessoa',
            'financeiro': 'email_financeiro'
        })

        # 3. Lógica de Negócio (Exemplos)
        df['porte_empresa'] = 'NAO_CALCULADO'
        df['categoria_risco'] = 'NAO_AVALIADO'

        df_dt = pd.to_datetime(df['data_criacao'], errors='coerce')
        df['tempo_cliente_dias'] = (datetime.now() - df_dt).dt.days

        # 4. Colunas para o Hash do SCD2 (usando os nomes finais da Silver)
        colunas_hash = [
            'cnpj_cpf_nk', 'razao_social', 'tipo_pessoa', 'status', 'status_qualificacao',
            'grupo', 'responsavel_conta', 'email_financeiro', 'corte', 'faixa',
            'cnpj_cpf_formatado', 'porte_empresa', 'categoria_risco'
        ]

        df['hash_registro'] = df.apply(lambda row: self.calcular_hash_registro(row, colunas_hash), axis=1)

        # 5. Selecionar APENAS colunas que existem na Silver (evitar erro de coluna inexistente)
        colunas_silver = [
            'cnpj_cpf_nk', 'razao_social', 'tipo_pessoa', 'status', 'status_qualificacao',
            'data_criacao', 'grupo', 'responsavel_conta', 'email_financeiro', 'corte', 'faixa',
            'cnpj_cpf_formatado', 'porte_empresa', 'tempo_cliente_dias', 'categoria_risco',
            'hash_registro'
        ]

        return df[colunas_silver]

    def validar_qualidade(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        """Valida a qualidade dos dados transformados antes da carga."""
        erros = []
        if df['cnpj_cpf_nk'].isnull().any():
            erros.append("Campo 'cnpj_cpf_nk' (chave natural) não pode ser nulo.")
        
        # Valida se PJs (Pessoa Jurídica) possuem razão social
        pj_sem_razao = df[(df['tipo_pessoa'] == 'PJ') & (df['razao_social'].isnull())]
        if not pj_sem_razao.empty:
            erros.append(f"{len(pj_sem_razao)} clientes PJ estão sem razão social.")

        return len(erros) == 0, erros


if __name__ == '__main__':
    # Permite a execução do script diretamente
    sys.exit(TransformDimCliente().executar())