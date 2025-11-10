# python/run_silver_transformations.py
"""
Orquestra as transformações Silver com dependências
"""

from transformers.silver import (
    TransformDimClientes,
    TransformDimUsuarios,
    TransformDimTempo,
    TransformFactFaturamento
)

class SilverOrchestrator:
    def __init__(self):
        # Define ordem de execução (dimensões antes de fatos)
        self.pipeline = [
            ('dim_tempo', TransformDimTempo),
            ('dim_clientes', TransformDimClientes),
            ('dim_usuarios', TransformDimUsuarios),
            ('fact_faturamento', TransformFactFaturamento)
        ]
    
    def executar(self, paralelo: bool = False):
        """Executa pipeline respeitando dependências"""
        pass
