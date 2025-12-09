"""
Ingestor específico para os dados de usuários.

Este módulo, uma implementação do `BaseIngestor`, é responsável por processar
os arquivos de usuários. Ele define os metadados necessários para a ingestão,
como o nome do ingestor, a tabela de destino e as colunas obrigatórias.

O mapeamento de colunas neste ingestor é direto, pois os nomes das colunas
no arquivo de origem já correspondem aos da tabela de destino.
"""
from python.core.base_ingestor import BaseIngestor

class IngestUsuarios(BaseIngestor):
    """
    Ingestor para os dados de usuários.
    Herda de `BaseIngestor` e o configura para a tabela de usuários.
    """
    def __init__(self):
        """
        Inicializa o ingestor de usuários.
        - `name`: Identificador para logs e busca de templates.
        - `target_table`: Tabela de destino no schema bronze.
        - `mandatory_cols`: Colunas essenciais para a validade de um registro.
        """
        super().__init__(
            name="usuarios",
            target_table="bronze.usuarios",
            mandatory_cols=['consultor', 'cargo']
        )

    def get_column_mapping(self):
        """
        Retorna o mapeamento de colunas do CSV para o banco de dados.

        Neste caso, o mapeamento é vazio, pois os nomes das colunas no arquivo
        de origem são idênticos aos da tabela de destino. A estrutura é mantida
        por consistência com a classe base.

        Returns:
            dict: Um dicionário vazio.
        """
        return {
            # Nomes das colunas no CSV e no banco de dados são idênticos,
            # então nenhum mapeamento é necessário.
        }

if __name__ == "__main__":
    # Permite que o ingestor seja executado como um script autônomo
    IngestUsuarios().run("usuario.csv")
