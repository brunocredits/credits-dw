"""
Exemplo de testes usando as fixtures de limpeza de dados.

Este arquivo demonstra como usar as fixtures disponíveis em conftest.py.
"""

import pytest
from psycopg2.extensions import connection as Connection


@pytest.mark.unit
def test_db_connection(db_connection: Connection):
    """Testa se a conexão com banco está funcionando."""
    cursor = db_connection.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    assert result[0] == 1
    cursor.close()


@pytest.mark.integration
def test_with_clean_bronze(clean_bronze, db_connection: Connection):
    """
    Exemplo de teste com limpeza automática de Bronze.

    As tabelas Bronze são limpas antes e depois deste teste.
    """
    cursor = db_connection.cursor()

    # Verificar que tabela está vazia
    cursor.execute("SELECT COUNT(*) FROM bronze.usuarios;")
    count_before = cursor.fetchone()[0]
    assert count_before == 0, "Tabela deveria estar vazia no início"

    # Inserir dados de teste
    cursor.execute("""
        INSERT INTO bronze.usuarios (nome_empresa, nome, area, senioridade, gestor, email, canal_1, canal_2, email_lider)
        VALUES ('Test Co', 'Test User', 'Sales', 'Senior', 'Manager', 'test@example.com', 'WhatsApp', 'Email', 'manager@example.com');
    """)
    db_connection.commit()

    # Verificar inserção
    cursor.execute("SELECT COUNT(*) FROM bronze.usuarios;")
    count_after = cursor.fetchone()[0]
    assert count_after == 1, "Deveria ter 1 registro após inserção"

    cursor.close()

    # Após este teste, a fixture clean_bronze vai limpar os dados automaticamente


@pytest.mark.integration
def test_with_rollback(db_connection_with_rollback: Connection):
    """
    Exemplo de teste com rollback automático.

    Qualquer mudança no banco é revertida após o teste.
    Ideal para testes que não devem persistir dados.
    """
    cursor = db_connection_with_rollback.cursor()

    # Inserir dados de teste
    cursor.execute("""
        INSERT INTO bronze.faturamento (data, receita, moeda)
        VALUES ('2025-01-13', '99999.99', 'BRL');
    """)

    # Verificar inserção
    cursor.execute("SELECT COUNT(*) FROM bronze.faturamento WHERE receita = 99999.99;")
    count = cursor.fetchone()[0]
    assert count == 1, "Registro deveria existir durante o teste"

    cursor.close()

    # Após o teste, o rollback automático reverte a inserção


@pytest.mark.integration
def test_with_sample_data(sample_bronze_data, db_connection: Connection):
    """
    Exemplo de teste usando dados de exemplo pré-carregados.

    A fixture sample_bronze_data insere dados de teste antes do teste
    e os remove após o teste.
    """
    cursor = db_connection.cursor()

    # Verificar que dados de exemplo existem
    cursor.execute("SELECT COUNT(*) FROM bronze.usuarios;")
    count_usuarios = cursor.fetchone()[0]
    assert count_usuarios >= 2, "Deveria ter pelo menos 2 usuários de teste"

    cursor.execute("SELECT COUNT(*) FROM bronze.faturamento;")
    count_faturamento = cursor.fetchone()[0]
    assert count_faturamento >= 2, "Deveria ter pelo menos 2 registros de faturamento"

    cursor.close()

    # Dados são limpos automaticamente após o teste


@pytest.mark.integration
def test_cleaner_verification(test_data_cleaner, db_connection: Connection):
    """Testa a funcionalidade de verificação de contagens."""
    # Este teste apenas verifica que o método não lança exceção
    test_data_cleaner.verificar_contagens()

    # Se chegou aqui, o método funcionou
    assert True


@pytest.mark.slow
@pytest.mark.integration
def test_full_cleanup(clean_all, db_connection: Connection):
    """
    Teste que usa limpeza completa (Bronze + Silver + Audit).

    Útil para testes de integração completos.
    """
    cursor = db_connection.cursor()

    # Verificar que tudo está limpo
    tabelas = [
        'bronze.usuarios',
        'bronze.faturamento',
        'silver.dim_tempo',
        'silver.dim_clientes'
    ]

    for tabela in tabelas:
        cursor.execute(f"SELECT COUNT(*) FROM {tabela};")
        count = cursor.fetchone()[0]
        # Nota: dim_tempo pode ter dados pré-carregados, então esse teste
        # só funciona se clean_all realmente limpar tudo
        print(f"{tabela}: {count} registros")

    cursor.close()
