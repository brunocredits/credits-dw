from python.core.base_ingestor import BaseIngestor

class IngestBaseOficial(BaseIngestor):
    def __init__(self):
        super().__init__(
            name="base_oficial",
            target_table="bronze.base_oficial",
            mandatory_cols=['cnpj', 'status', 'manter_no_baseline']
        )

    def get_column_mapping(self):
        """
        Maps CSV headers to database columns.
        base_oficial.csv already has perfect snake_case headers,
        so we return an empty dict (no mapping needed).
        """
        return {
            # Todos os headers tem que seguir o que está no banco de dados:
            # cnpj, status, manter_no_baseline, razao_social, nome_fantasia,
            # vertical, canal_1, canal_2, lider, responsavel, empresa,
            # grupo, corte, segmento, obs
        }

if __name__ == "__main__":
    IngestBaseOficial().run("base_oficial.csv")
