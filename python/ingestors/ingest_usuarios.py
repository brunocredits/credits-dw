from python.core.base_ingestor import BaseIngestor

class IngestUsuarios(BaseIngestor):
    def __init__(self):
        super().__init__(
            name="usuarios",
            target_table="bronze.usuarios",
            mandatory_cols=['consultor', 'cargo'],
            money_cols=['meta', 'mensal_fidelidade', 'meta_anual', 'meta_jan', 'meta_fev', 'meta_mar', 'meta_abr', 'meta_mai', 'meta_jun', 'meta_jul', 'meta_ago', 'meta_set', 'meta_out', 'meta_nov', 'meta_dez']
        )

    def get_column_mapping(self):
        return {
            'consultor_a': 'consultor',
            'mensal_fidelidade': 'mensal_fidelidade',
            'acesso_diretoria': 'acesso_diretoria'
        }

if __name__ == "__main__":
    IngestUsuarios().run("usuario.csv")
