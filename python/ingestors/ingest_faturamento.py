from python.core.base_ingestor import BaseIngestor

class IngestFaturamento(BaseIngestor):
    def __init__(self):
        super().__init__(
            name="faturamento",
            target_table="bronze.faturamento",
            mandatory_cols=[
                'nota_fiscal', 
                'nome_fantasia', 
                'valor_conta', 
                'valor_liquido',
                'impostos', 
                'desconto', 
                'juros_multa', 
                'valor_recebido',
                'valor_receber', 
                'vencimento', 
                'data_emissao', 
                'razao_social',
                'cnpj', 
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
            'impostos': 'impostos_retidos',
            'valor_receber': 'valor_a_receber',
            
            # Other fields
            'obs': 'observacao',
            
            # Fields that match DB columns (no mapping needed):
            # numero_documento, parcela, nota_fiscal, previsao_recebimento,
            # ultimo_recebimento, valor_conta, valor_liquido, desconto,
            # juros_multa, valor_recebido, categoria, operacao, vendedor,
            # projeto, conta_corrente, numero_boleto, tipo_documento,
            # vencimento, data_emissao, ultima_alteracao, incluido_por,
            # alterado_por, data_fat, empresa, ms
            
            # Fields in CSV but not in DB (will be ignored):
            # ano, inclusao
        }

if __name__ == "__main__":
    IngestFaturamento().run("faturamento.csv")