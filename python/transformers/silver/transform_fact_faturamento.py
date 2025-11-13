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
        df['data_ref'] = pd.to_datetime(df['data']).dt.date
        dim_tempo = pd.read_sql("SELECT sk_data, data_completa FROM silver.dim_tempo", self.conn)
        df = df.merge(dim_tempo, left_on='data_ref', right_on='data_completa', how='left')
        df['sk_cliente'] = 1
        df['sk_usuario'] = 1
        df['sk_canal'] = 1
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
        if df['sk_data'].isnull().any():
            erros.append("Datas nao encontradas na dim_tempo")
        if df['valor_bruto'].isnull().any():
            erros.append("Valores nulos encontrados")
        return len(erros) == 0, erros

    def executar(self) -> int:
        from utils.db_connection import get_db_connection
        self.conn = get_db_connection()
        try:
            return super().executar()
        finally:
            self.conn.close()

if __name__ == '__main__':
    TransformFactFaturamento().executar()
