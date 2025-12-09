"""
Ingestor específico para os dados de faturamento.

Este módulo é uma implementação concreta do `BaseIngestor` e é desenhado
especificamente para processar arquivos de faturamento. Ele define o nome do
ingestor, a tabela de destino no banco de dados e as colunas que são
consideradas obrigatórias para um registro ser válido.

Além disso, implementa o método `get_column_mapping`, que é crucial para
traduzir os nomes das colunas do arquivo CSV para os nomes correspondentes
na tabela `bronze.faturamento`.
"""
from python.core.base_ingestor import BaseIngestor

class IngestFaturamento(BaseIngestor):
    """
    Ingestor para os dados de faturamento.
    Herda de `BaseIngestor` e especializa o processo para a tabela de faturamento.
    """
    def __init__(self):
        """
        Inicializa o ingestor de faturamento, definindo metadados essenciais.
        - `name`: Usado para logging e para encontrar o template de validação.
        - `target_table`: A tabela de destino no schema bronze.
        - `mandatory_cols`: Colunas que não podem ser nulas ou vazias para que
          um registro seja considerado válido.
        """
        super().__init__(
            name="faturamento",
            target_table="bronze.faturamento",
            mandatory_cols=[
                'nota_fiscal', 
                'cliente_nome_fantasia', 
                'valor_da_conta', 
                'valor_liquido',
                'impostos_retidos', 
                'descontos', 
                'juros_multa', 
                'valor_recebido',
                'valor_a_receber', 
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
        Mapeia os nomes das colunas do arquivo CSV para os nomes das colunas no banco de dados.
        
        Este mapeamento é essencial para corrigir inconsistências, erros de digitação
        ou simplesmente para alinhar os nomes do arquivo de origem com o modelo de
        dados do data warehouse.

        Returns:
            dict: Um dicionário onde as chaves são os nomes das colunas no CSV e os
                  valores são os nomes das colunas correspondentes no banco de dados.
        """
        return {
            # Mapeamento de colunas com nomes diferentes
            'nome_fantasia': 'cliente_nome_fantasia',
            
            # Correção de erro de digitação no template
            'categorioa': 'categoria',
            
            # As colunas restantes têm o mesmo nome no CSV e no banco,
            # portanto, não precisam de mapeamento explícito.
        }

if __name__ == "__main__":
    # Permite a execução do ingestor diretamente como um script
    IngestFaturamento().run("faturamento.csv")