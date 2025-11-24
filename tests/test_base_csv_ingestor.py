"""
Testes unitários para BaseCSVIngestor
"""
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os

# Adicionar path do projeto
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / 'python'))

from ingestors.csv.base_csv_ingestor import BaseCSVIngestor


class TestIngestor(BaseCSVIngestor):
    """Ingestor de teste para validar classe base"""

    def __init__(self, tabela_destino='bronze.faturamento', arquivo_nome='test.csv'):
        super().__init__(
            script_name='test_ingestor.py',
            tabela_destino=tabela_destino,
            arquivo_nome=arquivo_nome,
            input_subdir='onedrive'
        )

    def get_column_mapping(self):
        return {
            'Data': 'data',
            'Receita': 'receita',
            'Moeda': 'moeda'
        }

    def get_bronze_columns(self):
        return ['data', 'receita', 'moeda']


class TestBaseCSVIngestor:
    """Suite de testes para BaseCSVIngestor"""

    @pytest.fixture
    def temp_csv_file(self, tmp_path):
        """Cria arquivo CSV temporário para testes"""
        csv_content = """Data;Receita;Moeda
2024-01-15;15000.00;BRL
2024-01-20;5000.00;BRL
2024-02-20;25000.00;BRL"""

        csv_file = tmp_path / "onedrive" / "test.csv"
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        csv_file.write_text(csv_content)

        return csv_file

    @pytest.fixture
    def mock_ingestor(self, tmp_path):
        """Cria ingestor mockado para testes"""
        with patch('ingestors.csv.base_csv_ingestor.get_paths_config') as mock_config:
            # Mock paths config
            mock_paths = Mock()
            mock_paths.data_input_dir = tmp_path
            mock_paths.data_processed_dir = tmp_path / "processed"
            mock_config.return_value = mock_paths

            # Mock CSV config
            with patch('ingestors.csv.base_csv_ingestor.get_csv_config') as mock_csv_config:
                mock_csv = Mock()
                mock_csv.encoding = 'utf-8-sig'
                mock_csv.separator = ';'
                mock_csv.date_format = '%Y-%m-%d'
                mock_csv.na_values = ['', 'NULL', 'null']
                mock_csv_config.return_value = mock_csv

                # Mock ETL config
                with patch('ingestors.csv.base_csv_ingestor.get_etl_config') as mock_etl_config:
                    mock_etl = Mock()
                    mock_etl.batch_insert_size = 1000
                    mock_etl_config.return_value = mock_etl

                    ingestor = TestIngestor()
                    yield ingestor

    def test_init_valida_tabela_whitelist(self):
        """Testa se __init__ valida tabelas na whitelist"""
        with pytest.raises(ValueError, match="Tabela não permitida"):
            TestIngestor(tabela_destino='public.hacker_table')

    def test_get_date_columns_detecta_prefixos(self, mock_ingestor):
        """Testa detecção automática de colunas de data"""
        # Modificar mapeamento para incluir data_
        mock_ingestor.get_bronze_columns = lambda: ['data', 'data_criacao', 'dt_update', 'receita']

        date_cols = mock_ingestor.get_date_columns()

        assert 'data' in date_cols
        assert 'data_criacao' in date_cols
        assert 'dt_update' in date_cols
        assert 'receita' not in date_cols

    def test_validar_arquivo_sucesso(self, mock_ingestor, temp_csv_file):
        """Testa validação de arquivo existente"""
        mock_ingestor.arquivo_path = temp_csv_file

        resultado = mock_ingestor.validar_arquivo()

        assert resultado is True

    def test_validar_arquivo_nao_existe(self, mock_ingestor):
        """Testa validação de arquivo inexistente"""
        mock_ingestor.arquivo_path = Path("/nao/existe.csv")

        with pytest.raises(FileNotFoundError, match="Arquivo não encontrado"):
            mock_ingestor.validar_arquivo()

    def test_ler_csv_sucesso(self, mock_ingestor, temp_csv_file):
        """Testa leitura de CSV com sucesso"""
        mock_ingestor.arquivo_path = temp_csv_file

        df = mock_ingestor.ler_csv()

        assert len(df) == 3
        assert 'Data' in df.columns
        assert 'Receita' in df.columns
        assert 'Moeda' in df.columns

    def test_ler_csv_arquivo_vazio(self, mock_ingestor, tmp_path):
        """Testa leitura de CSV vazio"""
        empty_csv = tmp_path / "onedrive" / "empty.csv"
        empty_csv.parent.mkdir(parents=True, exist_ok=True)
        empty_csv.write_text("")

        mock_ingestor.arquivo_path = empty_csv

        with pytest.raises(pd.errors.EmptyDataError):
            mock_ingestor.ler_csv()

    def test_validar_colunas_sucesso(self, mock_ingestor):
        """Testa validação de colunas presentes"""
        df = pd.DataFrame({
            'Data': ['2024-01-15'],
            'Receita': [15000],
            'Moeda': ['BRL']
        })

        resultado = mock_ingestor.validar_colunas(df)

        assert resultado is True

    def test_validar_colunas_faltando(self, mock_ingestor):
        """Testa validação de colunas faltando"""
        df = pd.DataFrame({
            'Data': ['2024-01-15'],
            'Receita': [15000]
            # Moeda faltando
        })

        with pytest.raises(ValueError, match="Colunas faltando"):
            mock_ingestor.validar_colunas(df)

    def test_transformar_para_bronze_renomeia_colunas(self, mock_ingestor):
        """Testa renomeação de colunas no transformar_para_bronze"""
        df = pd.DataFrame({
            'Data': ['2024-01-15'],
            'Receita': ['15000'],
            'Moeda': ['BRL']
        })

        registros, colunas = mock_ingestor.transformar_para_bronze(df)

        assert 'data' in colunas
        assert 'receita' in colunas
        assert 'moeda' in colunas
        assert 'Data' not in colunas

    def test_transformar_para_bronze_adiciona_colunas_faltantes(self, mock_ingestor):
        """Testa adição de colunas faltantes como NULL"""
        mock_ingestor.get_bronze_columns = lambda: ['data', 'receita', 'moeda', 'extra_col']

        df = pd.DataFrame({
            'Data': ['2024-01-15'],
            'Receita': ['15000'],
            'Moeda': ['BRL']
        })

        registros, colunas = mock_ingestor.transformar_para_bronze(df)

        assert 'extra_col' in colunas
        # Verificar se valores são None
        assert registros[0][-1] is None  # extra_col deve ser None

    def test_formatar_datas_validas(self, mock_ingestor):
        """Testa formatação de datas válidas"""
        df = pd.DataFrame({
            'data': ['15/01/2024', '2024-01-20', '20/02/2024']
        })

        df_formatado = mock_ingestor._formatar_datas(df)

        assert df_formatado['data'].iloc[0] == '2024-01-15'
        assert df_formatado['data'].iloc[1] == '2024-01-20'
        assert df_formatado['data'].iloc[2] == '2024-02-20'

    def test_formatar_datas_invalidas(self, mock_ingestor):
        """Testa formatação de datas inválidas"""
        df = pd.DataFrame({
            'data': ['data_invalida', '99/99/9999', None]
        })

        df_formatado = mock_ingestor._formatar_datas(df)

        # Datas inválidas devem ser None
        assert df_formatado['data'].iloc[0] is None
        assert df_formatado['data'].iloc[1] is None
        assert df_formatado['data'].iloc[2] is None

    @patch('ingestors.csv.base_csv_ingestor.execute_values')
    @patch('ingestors.csv.base_csv_ingestor.get_cursor')
    def test_inserir_bronze_truncate_reload(self, mock_cursor, mock_execute_values, mock_ingestor):
        """Testa estratégia TRUNCATE/RELOAD"""
        # Mock connection
        mock_conn = Mock()
        mock_cur = MagicMock()
        mock_cursor.return_value.__enter__.return_value = mock_cur

        registros = [
            ['2024-01-15', 15000, 'BRL'],
            ['2024-01-20', 5000, 'BRL']
        ]
        colunas = ['data', 'receita', 'moeda']

        linhas_inseridas = mock_ingestor.inserir_bronze(mock_conn, registros, colunas)

        # Verificar TRUNCATE foi chamado
        assert mock_cur.execute.called
        truncate_call = str(mock_cur.execute.call_args_list[0])
        assert 'TRUNCATE' in truncate_call

        # Verificar INSERT foi chamado
        assert mock_execute_values.called
        assert linhas_inseridas == 2

    def test_mover_para_processed(self, mock_ingestor, tmp_path):
        """Testa arquivamento de arquivo processado"""
        # Criar arquivo temporário
        source_file = tmp_path / "onedrive" / "test.csv"
        source_file.parent.mkdir(parents=True, exist_ok=True)
        source_file.write_text("test content")

        processed_dir = tmp_path / "processed"
        processed_dir.mkdir(parents=True, exist_ok=True)

        mock_ingestor.arquivo_path = source_file
        mock_ingestor.processed_dir = processed_dir

        mock_ingestor.mover_para_processed()

        # Verificar arquivo foi movido
        assert not source_file.exists()
        # Verificar existe arquivo no diretório processed com timestamp
        processed_files = list(processed_dir.glob("*test.csv"))
        assert len(processed_files) == 1


@pytest.mark.integration
class TestBaseCSVIngestorIntegration:
    """Testes de integração (requerem banco de dados)"""

    def test_executar_pipeline_completo(self):
        """Testa pipeline completo de ingestão"""
        # TODO: Implementar teste de integração completo
        # Requer mock do banco de dados ou ambiente de teste
        pytest.skip("Teste de integração requer banco de dados")
