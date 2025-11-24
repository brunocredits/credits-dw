import pandas as pd
from datetime import datetime
from transformers.base_transformer import BaseSilverTransformer

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
        from utils.db_connection import get_db_connection
        import logging

        logger = logging.getLogger(__name__)

        df['data_ref'] = pd.to_datetime(df['data']).dt.date

        # Obter dimensões para lookup de FKs
        with get_db_connection() as conn:
            # 1. Resolver sk_data via JOIN com dim_tempo
            dim_tempo = pd.read_sql("SELECT sk_data, data_completa FROM silver.dim_tempo", conn)

            # 2. Buscar todas dimensões necessárias
            dim_clientes = pd.read_sql(
                "SELECT sk_cliente, nk_cnpj_cpf FROM silver.dim_clientes WHERE flag_ativo = TRUE",
                conn
            )
            dim_usuarios = pd.read_sql(
                "SELECT sk_usuario, nk_usuario FROM silver.dim_usuarios WHERE flag_ativo = TRUE",
                conn
            )
            dim_canal = pd.read_sql(
                "SELECT sk_canal, nome_canal FROM silver.dim_canal ORDER BY sk_canal LIMIT 1",
                conn
            )

        # JOIN com dim_tempo pela data
        df = df.merge(dim_tempo, left_on='data_ref', right_on='data_completa', how='left')

        # IMPORTANTE: CSV bronze.faturamento não contém identificadores de cliente ou usuário
        # Estratégia temporária: usar primeiro cliente/usuário disponível
        # TODO: Adicionar colunas cnpj_cliente e email_usuario nos CSVs de faturamento
        if not dim_clientes.empty:
            df['sk_cliente'] = dim_clientes['sk_cliente'].iloc[0]
            logger.warning(f"Usando cliente padrão (sk_cliente={dim_clientes['sk_cliente'].iloc[0]}) "
                         "pois CSV faturamento não contém identificador de cliente")
        else:
            df['sk_cliente'] = None

        if not dim_usuarios.empty:
            df['sk_usuario'] = dim_usuarios['sk_usuario'].iloc[0]
            logger.warning(f"Usando usuário padrão (sk_usuario={dim_usuarios['sk_usuario'].iloc[0]}) "
                         "pois CSV faturamento não contém identificador de usuário")
        else:
            df['sk_usuario'] = None

        if not dim_canal.empty:
            df['sk_canal'] = dim_canal['sk_canal'].iloc[0]
        else:
            df['sk_canal'] = None
        df['valor_bruto'] = df['receita']
        df['valor_desconto'] = 0
        df['valor_liquido'] = df['receita']
        df['valor_imposto'] = df['receita'] * 0.15
        df['valor_comissao'] = df['receita'] * 0.05
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
        df['hash_transacao'] = df.apply(lambda r: self.calcular_hash_registro(r, ['data_ref', 'valor_bruto', 'moeda']), axis=1)
        cols = ['sk_cliente', 'sk_usuario', 'sk_data', 'sk_canal', 'valor_bruto', 'valor_desconto',
                'valor_liquido', 'valor_imposto', 'valor_comissao', 'quantidade', 'numero_parcelas',
                'numero_documento', 'tipo_documento', 'moeda', 'forma_pagamento', 'status_pagamento',
                'data_vencimento', 'data_pagamento', 'origem_dado', 'data_processamento', 'hash_transacao']
        return df[cols]

    def validar_qualidade(self, df: pd.DataFrame):
        erros = []
        warnings = []

        # Validações críticas (bloqueiam execução)
        if df['sk_data'].isnull().any():
            datas_faltando = df[df['sk_data'].isnull()]['data_ref'].unique()
            erros.append(f"Datas não encontradas na dim_tempo: {datas_faltando}")

        if df['valor_bruto'].isnull().any():
            erros.append(f"Valores brutos nulos encontrados: {df['valor_bruto'].isnull().sum()} registros")

        # Validações importantes (permitem execução mas geram warning)
        if df['sk_cliente'].isnull().any():
            warnings.append(f"AVISO: {df['sk_cliente'].isnull().sum()} registros sem sk_cliente (dim_clientes vazia?)")

        if df['sk_usuario'].isnull().any():
            warnings.append(f"AVISO: {df['sk_usuario'].isnull().sum()} registros sem sk_usuario (dim_usuarios vazia?)")

        if df['sk_canal'].isnull().any():
            warnings.append(f"AVISO: {df['sk_canal'].isnull().sum()} registros sem sk_canal (dim_canal vazia?)")

        # Logar warnings sem bloquear execução
        if warnings:
            import logging
            logger = logging.getLogger(__name__)
            for warning in warnings:
                logger.warning(warning)

        return len(erros) == 0, erros

if __name__ == '__main__':
    TransformFactFaturamento().executar()
