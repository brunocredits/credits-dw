
import pandas as pd
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from python.utils.db_connection import get_db_connection
from python.core.base_ingestor import BaseIngestor

class IngestData(BaseIngestor):
    def __init__(self):
        super().__init__(
            name="data",
            target_table="bronze.data",
            mandatory_cols=['data']
        )

    def get_column_mapping(self):
        return {}

    def run(self):
        conn = get_db_connection()
        
        start_date = datetime(2015, 1, 1)
        end_date = datetime(2030, 12, 31)
        
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append({
                'data': current_date,
                'ano': current_date.year,
                'mes': current_date.month,
                'dia': current_date.day,
                'dia_semana': current_date.strftime('%A'),
                'semana_ano': current_date.isocalendar()[1],
                'trimestre': (current_date.month - 1) // 3 + 1
            })
            current_date += timedelta(days=1)
            
        df = pd.DataFrame(dates)
        
        db_cols = ['data', 'ano', 'mes', 'dia', 'dia_semana', 'semana_ano', 'trimestre']
        
        self.copy_to_db(conn, df, self.target_table, db_cols)
        
        conn.close()

if __name__ == "__main__":
    IngestData().run()
