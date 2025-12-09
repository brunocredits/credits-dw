"""
Ingestor específico para os dados da base oficial de clientes.

Este módulo é uma implementação do `BaseIngestor` para processar os arquivos
da base oficial. Ele define os metadados para a ingestão, como o nome, a
tabela de destino e as colunas obrigatórias.

Assim como o ingestor de usuários, o mapeamento de colunas é direto, pois
os nomes no arquivo de origem já estão alinhados com o esquema do banco de dados.
"""
from python.core.base_ingestor import BaseIngestor

class IngestBaseOficial(BaseIngestor):
    """
    Ingestor para os dados da base oficial de clientes.
    Herda de `BaseIngestor` e o especializa para a tabela `bronze.base_oficial`.
    """
    def __init__(self):
        """
        Inicializa o ingestor da base oficial.
        - `name`: Usado para logs e para localizar o template de validação.
        - `target_table`: A tabela de destino no schema bronze.
        - `mandatory_cols`: Colunas que não podem ser nulas para um registro ser válido.
        """
        super().__init__(
            name="base_oficial",
            target_table="bronze.base_oficial",
            mandatory_cols=[
                'cnpj', 'status', 'manter_no_baseline', 'razao_social',
                'nome_fantasia', 'canal_1', 'canal_2', 'lider', 'responsavel',
                'empresa', 'grupo', 'corte', 'segmento', 'obs'
            ]
        )

    def get_column_mapping(self):
        """
        Retorna o mapeamento de colunas do CSV para o banco de dados.

        Neste caso, o mapeamento é vazio, pois os nomes das colunas no arquivo
        de origem são idênticos aos da tabela de destino.

        Returns:
            dict: Um dicionário vazio.
        """
        return {}

if __name__ == "__main__":
    # Permite a execução do ingestor como um script independente
    IngestBaseOficial().run("base_oficial.csv")
