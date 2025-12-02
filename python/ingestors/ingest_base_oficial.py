from python.core.base_ingestor import BaseIngestor

class IngestBaseOficial(BaseIngestor):
    def __init__(self):
        super().__init__(
            name="base_oficial",
            target_table="bronze.base_oficial",
            mandatory_cols=['cnpj', 'status', 'manter_no_baseline'],
            money_cols=[] # No money cols in strict version
        )

    def get_column_mapping(self):
        return {
            'razao_social': 'razao_social',
            'nome_fantasia': 'nome_fantasia',
            # Maps specific requested columns to DB columns
            'manter_no_baselinse': 'manter_no_baseline', # Fix typo in input
            'responsavel': 'responsavel'
        }

if __name__ == "__main__":
    IngestBaseOficial().run("base_oficial.csv")