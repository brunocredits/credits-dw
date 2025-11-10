# python/transformers/silver/transform_dim_clientes.py
"""
Dimensão Clientes com histórico de mudanças (SCD2)
Combina dados de: bronze.contas_base_oficial
"""

class TransformDimClientes(BaseSilverTransformer):
    def __init__(self):
        super().__init__(
            script_name='transform_dim_clientes.py',
            tabela_origem='bronze.contas_base_oficial',
            tabela_destino='silver.dim_clientes',
            tipo_carga='scd2'
        )
    
    def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transformações:
        - Padronizar CNPJ/CPF (remover pontuação)
        - Criar chave surrogate (sk_cliente)
        - Adicionar campos SCD2 (data_inicio, data_fim, flag_ativo)
        - Limpar e padronizar nomes
        - Enriquecer com categoria de cliente
        """
        pass
