from python.core.base_ingestor import BaseIngestor

class IngestUsuarios(BaseIngestor):
    def __init__(self):
        super().__init__(
            name="usuarios",
            target_table="bronze.usuarios",
            mandatory_cols=['consultor', 'cargo']
        )

    def get_column_mapping(self):
        """
        Maps CSV headers to database columns.
        Most headers match, only acesso_temporario needs mapping.
        """
        return {
            # All other fields match DB columns exactly:
            # cargo, status_vendedor, consultor, nivel, time,
            # acesso_vendedor, acesso_gerente, acesso_indireto,
            # acesso_diretoria, acesso_temporario
        }

if __name__ == "__main__":
    IngestUsuarios().run("usuario.csv")
