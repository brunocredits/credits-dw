"""
Testes unitários para BaseSilverTransformer
"""
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock
import hashlib

# Adicionar path do projeto
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / 'python'))

from transformers.base_transformer import BaseSilverTransformer


class TestTransformer(BaseSilverTransformer):
    """Transformador de teste para validar classe base"""

    def __init__(self, tipo_carga='full', chave_natural=None):
        super().__init__(
            script_name='test_transformer.py',
            tabela_origem='bronze.test',
            tabela_destino='silver.dim_test',
            tipo_carga=tipo_carga,
            chave_natural=chave_natural
        )

    def extrair_bronze(self, conn):
        return pd.DataFrame({
            'id': [1, 2, 3],
            'nome': ['A', 'B', 'C'],
            'valor': [100, 200, 300]
        })

    def aplicar_transformacoes(self, df):
        df['valor_dobro'] = df['valor'] * 2
        return df

    def validar_qualidade(self, df):
        erros = []
        if df['valor'].isnull().any():
            erros.append("Valores nulos encontrados")
        return len(erros) == 0, erros


class TestBaseSilverTransformer:
    """Suite de testes para BaseSilverTransformer"""

    def test_init_valida_tabela_origem(self):
        """Testa validação de tabela origem"""
        with pytest.raises(ValueError, match="Tabela origem não permitida"):
            BaseSilverTransformer(
                script_name='test.py',
                tabela_origem='public.hacker',
                tabela_destino='silver.dim_test',
                tipo_carga='full'
            )

    def test_init_valida_tabela_destino(self):
        """Testa validação de tabela destino"""
        with pytest.raises(ValueError, match="Tabela destino não permitida"):
            BaseSilverTransformer(
                script_name='test.py',
                tabela_origem='bronze.usuarios',
                tabela_destino='public.hacker',
                tipo_carga='full'
            )

    def test_init_valida_tipo_carga(self):
        """Testa validação de tipo de carga"""
        with pytest.raises(ValueError, match="Tipo de carga inválido"):
            BaseSilverTransformer(
                script_name='test.py',
                tabela_origem='bronze.usuarios',
                tabela_destino='silver.dim_test',
                tipo_carga='invalid_type'
            )

    def test_calcular_hash_registro(self):
        """Testa cálculo de hash MD5"""
        transformer = TestTransformer()

        row = pd.Series({
            'nome': 'João Silva',
            'email': 'joao@example.com',
            'idade': 30
        })

        hash_result = transformer.calcular_hash_registro(row, ['nome', 'email', 'idade'])

        # Verificar que é um hash MD5 válido (32 caracteres hex)
        assert len(hash_result) == 32
        assert all(c in '0123456789abcdef' for c in hash_result)

        # Verificar consistência do hash
        hash_result2 = transformer.calcular_hash_registro(row, ['nome', 'email', 'idade'])
        assert hash_result == hash_result2

    def test_calcular_hash_registro_valores_diferentes(self):
        """Testa que valores diferentes geram hashes diferentes"""
        transformer = TestTransformer()

        row1 = pd.Series({'nome': 'João', 'email': 'joao@example.com'})
        row2 = pd.Series({'nome': 'Maria', 'email': 'joao@example.com'})

        hash1 = transformer.calcular_hash_registro(row1, ['nome', 'email'])
        hash2 = transformer.calcular_hash_registro(row2, ['nome', 'email'])

        assert hash1 != hash2

    def test_calcular_hash_registro_ordem_importa(self):
        """Testa que ordem das colunas não afeta o hash"""
        transformer = TestTransformer()

        row = pd.Series({'nome': 'João', 'email': 'joao@example.com'})

        hash1 = transformer.calcular_hash_registro(row, ['nome', 'email'])
        hash2 = transformer.calcular_hash_registro(row, ['email', 'nome'])

        # Hashes devem ser diferentes porque ordem importa
        assert hash1 != hash2

    @patch('transformers.base_transformer.pd.read_sql')
    @patch('transformers.base_transformer.get_cursor')
    def test_processar_scd2_primeira_carga(self, mock_cursor, mock_read_sql):
        """Testa SCD Type 2 - primeira carga (sem registros anteriores)"""
        transformer = TestTransformer(tipo_carga='scd2', chave_natural='nk_id')

        # Mock: tabela destino vazia
        mock_read_sql.return_value = pd.DataFrame()

        # Mock connection
        mock_conn = Mock()

        # DataFrame novo
        df_novo = pd.DataFrame({
            'nk_id': [1, 2],
            'nome': ['João', 'Maria'],
            'valor': [100, 200],
            'hash_registro': ['abc123', 'def456']
        })

        df_result = transformer.processar_scd2(df_novo, mock_conn)

        # Verificar campos SCD2 foram adicionados
        assert 'data_inicio' in df_result.columns
        assert 'data_fim' in df_result.columns
        assert 'flag_ativo' in df_result.columns
        assert 'versao' in df_result.columns

        # Verificar valores
        assert all(df_result['flag_ativo'] == True)
        assert all(df_result['versao'] == 1)
        assert all(df_result['data_fim'].isnull())

    @patch('transformers.base_transformer.pd.read_sql')
    @patch('transformers.base_transformer.get_cursor')
    def test_processar_scd2_detecta_mudancas(self, mock_cursor, mock_read_sql):
        """Testa SCD Type 2 - detecta mudanças em registros existentes"""
        transformer = TestTransformer(tipo_carga='scd2', chave_natural='nk_id')

        # Mock: registros atuais na Silver
        mock_read_sql.return_value = pd.DataFrame({
            'sk_test': [1, 2],
            'nk_id': [1, 2],
            'nome': ['João', 'Maria'],
            'valor': [100, 200],
            'hash_registro': ['abc123', 'def456'],
            'flag_ativo': [True, True],
            'versao': [1, 1]
        })

        mock_conn = Mock()
        mock_cur = MagicMock()
        mock_cursor.return_value.__enter__.return_value = mock_cur

        # DataFrame novo com mudança no registro 1
        df_novo = pd.DataFrame({
            'nk_id': [1, 2],
            'nome': ['João Silva', 'Maria'],  # Nome mudou
            'valor': [150, 200],  # Valor mudou
            'hash_registro': ['xyz789', 'def456']  # Hash mudou
        })

        df_result = transformer.processar_scd2(df_novo, mock_conn)

        # Deve ter executado UPDATE para fechar registro antigo
        assert mock_cur.execute.called

        # Resultado deve ter apenas registros alterados
        assert len(df_result) > 0

    def test_processar_scd2_sem_chave_natural(self):
        """Testa que processar_scd2 falha sem chave natural"""
        transformer = TestTransformer(tipo_carga='scd2', chave_natural=None)

        df_novo = pd.DataFrame({'id': [1, 2]})
        mock_conn = Mock()

        with pytest.raises(ValueError, match="Chave natural obrigatória"):
            transformer.processar_scd2(df_novo, mock_conn)

    @patch('transformers.base_transformer.get_connection')
    def test_executar_tipo_carga_full(self, mock_get_connection):
        """Testa execução com tipo de carga FULL"""
        transformer = TestTransformer(tipo_carga='full')

        # Mock connection e cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_connection.return_value.__enter__.return_value = mock_conn

        # Executar
        resultado = transformer.executar()

        # Verificar TRUNCATE foi chamado
        assert mock_cursor.execute.called
        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        truncate_called = any('TRUNCATE' in call for call in calls)
        assert truncate_called

        # Verificar sucesso
        assert resultado == 0

    def test_validar_qualidade_sucesso(self):
        """Testa validação de qualidade com sucesso"""
        transformer = TestTransformer()

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'nome': ['A', 'B', 'C'],
            'valor': [100, 200, 300]
        })

        valido, erros = transformer.validar_qualidade(df)

        assert valido is True
        assert len(erros) == 0

    def test_validar_qualidade_falha(self):
        """Testa validação de qualidade com falhas"""
        transformer = TestTransformer()

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'nome': ['A', 'B', 'C'],
            'valor': [100, None, 300]  # Valor nulo
        })

        valido, erros = transformer.validar_qualidade(df)

        assert valido is False
        assert len(erros) > 0
        assert "Valores nulos encontrados" in erros

    def test_extrair_bronze(self):
        """Testa método abstrato extrair_bronze"""
        transformer = TestTransformer()
        mock_conn = Mock()

        df = transformer.extrair_bronze(mock_conn)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_aplicar_transformacoes(self):
        """Testa método abstrato aplicar_transformacoes"""
        transformer = TestTransformer()

        df_input = pd.DataFrame({
            'id': [1, 2],
            'nome': ['A', 'B'],
            'valor': [100, 200]
        })

        df_output = transformer.aplicar_transformacoes(df_input)

        assert 'valor_dobro' in df_output.columns
        assert df_output['valor_dobro'].tolist() == [200, 400]


@pytest.mark.integration
class TestBaseSilverTransformerIntegration:
    """Testes de integração (requerem banco de dados)"""

    def test_executar_pipeline_completo(self):
        """Testa pipeline completo de transformação"""
        # TODO: Implementar teste de integração completo
        pytest.skip("Teste de integração requer banco de dados")

    def test_scd2_end_to_end(self):
        """Testa SCD Type 2 end-to-end"""
        # TODO: Implementar teste de SCD2 completo
        pytest.skip("Teste de integração requer banco de dados")
