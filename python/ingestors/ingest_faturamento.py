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
        - `mandatory_cols`: Colunas críticas para o negócio (reduzido para evitar falsos positivos).
          
          Nota: Na camada Bronze, validações devem ser mínimas. Campos são marcados como
          obrigatórios apenas quando sua ausência tornaria o registro completamente inútil.
          Validações de negócio mais rigorosas devem ser feitas na camada Silver.
        """
        super().__init__(
            name="faturamento",
            target_table="bronze.faturamento",
            # Apenas campos absolutamente essenciais para identificação e processamento
            # de um registro de faturamento. Outros campos podem ser nulos na Bronze.
            mandatory_cols=[
                'numero_documento',  # Identificador único do documento
                'cnpj',              # Cliente (essencial para joins)
                'data_fat',          # Data de faturamento (essencial para análises temporais)
                'valor_da_conta',    # Valor principal do documento
                'empresa'            # Empresa emissora (multi-tenant)
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
            'nome_fantasia': 'cliente_nome_fantasia',  # Renomeia para clareza (especifica que é do cliente)
            'categorioa': 'categoria',                 # Corrige typo no arquivo fonte
        }

if __name__ == "__main__":
    # Permite a execução do ingestor diretamente como um script
    IngestFaturamento().run("faturamento.csv")