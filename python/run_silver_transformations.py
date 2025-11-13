#!/usr/bin/env python3
"""Executa transformações Silver em ordem de dependências"""
import sys
from transformers.silver.transform_dim_clientes import TransformDimClientes
from transformers.silver.transform_dim_usuarios import TransformDimUsuarios
from transformers.silver.transform_fact_faturamento import TransformFactFaturamento

def main():
    transformers = [
        ('dim_clientes', TransformDimClientes()),
        ('dim_usuarios', TransformDimUsuarios()),
        ('fact_faturamento', TransformFactFaturamento())
    ]

    print("=== Executando Transformações Silver ===\n")
    for nome, transformer in transformers:
        print(f"▶ Executando {nome}...")
        result = transformer.executar()
        if result != 0:
            print(f"✗ Erro em {nome}")
            return 1
        print(f"✓ {nome} concluído\n")

    print("=== Todas transformações concluídas com sucesso ===")
    return 0

if __name__ == '__main__':
    sys.exit(main())
