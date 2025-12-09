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
        - `mandatory_cols`: Campos essenciais da base oficial de clientes.
          
          Campos obrigatórios refletem a estrutura mínima necessária para identificar
          e categorizar um cliente na hierarquia comercial (empresa > grupo > lider > responsável).
        """
        super().__init__(
            name="base_oficial",
            target_table="bronze.base_oficial",
            mandatory_cols=[
                'cnpj',              # Identificador único do cliente
                'status',            # Status do cliente (ativo, inativo, etc.)
                'manter_no_baseline',# Flag de inclusão em relatórios baseline
                'razao_social',      # Nome legal da empresa
                'nome_fantasia',     # Nome comercial
                'canal_1',           # Canal de atuação primário
                'canal_2',           # Canal de atuação secundário
                'lider',             # Líder responsável (hierarquia comercial)
                'responsavel',       # Responsável direto pela conta
                'empresa',           # Empresa do grupo (multi-tenant)
                'grupo',             # Grupo comercial
                'corte',             # Corte/segmentação
                'segmento',          # Segmento de mercado
                'obs'                # Observações importantes
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
