"""
Configuração de fixtures do pytest para testes do Credits DW.

Fixtures disponíveis:
- db_connection: Conexão com banco de dados
- clean_bronze: Limpa tabelas Bronze antes e depois do teste
- clean_silver: Limpa tabelas Silver antes e depois do teste
- clean_all: Limpa tudo antes e depois do teste
"""

import pytest
import psycopg2
from psycopg2.extensions import connection as Connection
import sys
from pathlib import Path

# Adicionar path do projeto ao PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'python'))

from utils.db_connection import get_db_connection
from utils.test_data_cleaner import TestDataCleaner


@pytest.fixture(scope='function')
def db_connection() -> Connection:
    """
    Fixture que fornece conexão com banco de dados.

    Escopo: function (nova conexão para cada teste)
    Cleanup: Fecha conexão automaticamente após teste
    """
    conn = get_db_connection()
    yield conn
    conn.close()


@pytest.fixture(scope='function')
def db_connection_with_rollback() -> Connection:
    """
    Fixture que fornece conexão com rollback automático.

    Útil para testes que não devem persistir dados.
    Todas as mudanças são revertidas após o teste.

    Escopo: function
    Cleanup: Rollback automático + fecha conexão
    """
    conn = get_db_connection()
    conn.autocommit = False

    yield conn

    # Rollback de qualquer mudança
    conn.rollback()
    conn.close()


@pytest.fixture(scope='function')
def test_data_cleaner(db_connection: Connection) -> TestDataCleaner:
    """
    Fixture que fornece instância do TestDataCleaner.

    Escopo: function
    """
    return TestDataCleaner(db_connection)


@pytest.fixture(scope='function')
def clean_bronze(db_connection: Connection):
    """
    Fixture que limpa tabelas Bronze antes e depois do teste.

    Uso:
        def test_algo(clean_bronze):
            # Tabelas Bronze já estão limpas aqui
            ...
            # Serão limpas novamente após o teste
    """
    cleaner = TestDataCleaner(db_connection)

    # Setup: limpar antes do teste
    cleaner.limpar_bronze()

    yield

    # Teardown: limpar depois do teste
    cleaner.limpar_bronze()


@pytest.fixture(scope='function')
def clean_silver(db_connection: Connection):
    """
    Fixture que limpa tabelas Silver antes e depois do teste.

    Uso:
        def test_algo(clean_silver):
            # Tabelas Silver já estão limpas aqui
            ...
            # Serão limpas novamente após o teste
    """
    cleaner = TestDataCleaner(db_connection)

    # Setup: limpar antes do teste
    cleaner.limpar_silver()

    yield

    # Teardown: limpar depois do teste
    cleaner.limpar_silver()


@pytest.fixture(scope='function')
def clean_all(db_connection: Connection):
    """
    Fixture que limpa TODAS as tabelas antes e depois do teste.

    Uso:
        def test_algo(clean_all):
            # Todas tabelas já estão limpas aqui
            ...
            # Serão limpas novamente após o teste
    """
    cleaner = TestDataCleaner(db_connection)

    # Setup: limpar antes do teste
    cleaner.limpar_tudo()

    yield

    # Teardown: limpar depois do teste
    cleaner.limpar_tudo()


@pytest.fixture(scope='function')
def sample_bronze_data(db_connection: Connection):
    """
    Fixture que insere dados de exemplo nas tabelas Bronze.

    Útil para testes que precisam de dados Bronze pré-carregados.
    Dados são removidos após o teste.
    """
    cursor = db_connection.cursor()

    # Inserir dados de exemplo
    cursor.execute("""
        INSERT INTO bronze.usuarios (nome_empresa, nome, area, senioridade, gestor, email, canal_1, canal_2, email_lider)
        VALUES
            ('Empresa Teste', 'Usuario Teste 1', 'Vendas', 'Senior', 'Gestor Teste', 'teste1@example.com', 'WhatsApp', 'Email', 'gestor@example.com'),
            ('Empresa Teste 2', 'Usuario Teste 2', 'Marketing', 'Junior', 'Gestor Teste 2', 'teste2@example.com', 'Telefone', 'SMS', 'gestor2@example.com');
    """)

    cursor.execute("""
        INSERT INTO bronze.faturamento (data, receita, moeda)
        VALUES
            ('2025-01-01', '10000.00', 'BRL'),
            ('2025-01-02', '15000.00', 'BRL');
    """)

    cursor.execute("""
        INSERT INTO bronze.contas_base_oficial
        (cnpj_cpf, tipo, status, status_qualificação_da_conta, data_criacao, grupo, razao_social, responsavel_conta, financeiro, corte, faixa)
        VALUES
            ('12345678000190', 'PJ', 'Ativo', 'Qualificado', '2024-01-01', 'Grupo A', 'Empresa Teste LTDA', 'Responsável Teste', 'Adimplente', 'Premium', 'Grande');
    """)

    db_connection.commit()
    cursor.close()

    yield

    # Teardown: limpar dados de teste
    cleaner = TestDataCleaner(db_connection)
    cleaner.limpar_bronze()


# Configurações do pytest
def pytest_configure(config):
    """Configuração inicial do pytest."""
    config.addinivalue_line(
        "markers", "slow: marca testes lentos (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marca testes de integração"
    )
    config.addinivalue_line(
        "markers", "unit: marca testes unitários"
    )
