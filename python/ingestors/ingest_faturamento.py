from python.core.base_ingestor import BaseIngestor

class IngestFaturamento(BaseIngestor):
    def __init__(self):
        super().__init__(
            name="faturamento",
            target_table="bronze.faturamento",
            mandatory_cols=['numero_documento', 'cliente_nome_fantasia'],
            date_cols=[
                'previsao_recebimento', 'ultimo_recebimento', 'vencimento', 
                'data_emissao', 'ultima_alteracao', 'data_fat'
            ],
            money_cols=[
                'valor_conta', 'valor_liquido', 'impostos_retidos', 'desconto', 
                'juros_multa', 'valor_recebido', 'valor_a_receber'
            ]
        )

    def get_column_mapping(self):
        return {
            'numero_do_documento': 'numero_documento',
            'nota_fiscal_cupom_fiscal': 'nota_fiscal',
            'cliente_nome_fantasia_': 'cliente_nome_fantasia',
            'cliente_nome_fantasia': 'cliente_nome_fantasia',
            'valor_da_conta': 'valor_conta',
            'valor_liquido_': 'valor_liquido',
            'previsao_de_recebimento': 'previsao_recebimento',
            'juros_e_multa': 'juros_multa',
            'numero_do_boleto': 'numero_boleto',
            'tipo_de_documento': 'tipo_documento',
            'data_de_emissao': 'data_emissao',
            'cliente_razao_social': 'cliente_razao_social',
            'cliente_sem_pontuacao': 'cliente_sem_pontuacao',
            'tags_do_cliente': 'tags_cliente',
            'tags_do_cliente_1': 'tags_cliente', # Catch duplicate
            'data_fat': 'data_fat',
            'ms': 'ms'
        }

if __name__ == "__main__":
    IngestFaturamento().run("faturamento.csv")