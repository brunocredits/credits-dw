from python.core.base_ingestor import BaseIngestor

class IngestFaturamento(BaseIngestor):
    def __init__(self):
        super().__init__(
            name="faturamento",
            target_table="bronze.faturamento",
            mandatory_cols=[
                'nota_fiscal', 
                'cliente_nome_fantasia', 
                'valor_conta', 
                'valor_liquido',
                'impostos_retidos', 
                'desconto', 
                'juros_multa', 
                'valor_recebido',
                'valor_a_receber', 
                'vencimento', 
                'data_emissao', 
                'cliente_razao_social',
                'cliente_sem_pontuacao', 
                'data_fat', 
                'empresa'
            ]
        )

    def get_column_mapping(self):
        """
        Maps CSV headers (after user corrections) to database columns.
        Only includes mappings where CSV header != DB column.
        """
        return {
            # Status field
            'a_vencer_boleto_gerado': 'status',
            
            # Cliente fields
            'nome_fantasia': 'cliente_nome_fantasia',
            'razao_social': 'cliente_razao_social',
            'cnpj': 'cliente_sem_pontuacao',
            
            # Valor fields
            'valor_da_conta': 'valor_conta',
            'descontos': 'desconto',
            
            # Typo correction
            'categorioa': 'categoria',
            
            # Fields that match DB columns (no mapping needed):
            # numero_documento, parcela, nota_fiscal, previsao_recebimento,
            # ultimo_recebimento, valor_liquido, impostos_retidos,
            # juros_multa, valor_recebido, valor_a_receber, operacao, vendedor,
            # projeto, conta_corrente, numero_boleto, tipo_documento,
            # vencimento, data_emissao, ultima_alteracao, incluido_por,
            # alterado_por, data_fat, empresa, ms, observacao
        }

if __name__ == "__main__":
    IngestFaturamento().run("faturamento.csv")