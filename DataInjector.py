from google.cloud import bigquery
import pandas as pd
import traceback

class DataInjector:
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.bq_project_name = 'exame-prod'
        self.dataset_name = 'learn_worlds'

    def preparar_dados_para_insercao(self, df):
        campos_float = ['progress_rate', 'average_score_rate', 'time_on_course', 'completed_units', 'time_on_unit', 'score_on_unit', 'unit_duration', 'unit_progress_rate']
        for campo in campos_float:
            df[campo] = pd.to_numeric(df[campo], errors='coerce', downcast='float')
        campos_string = ['user_id', 'course_id', 'status', 'total_units', 'unit_id', 'unit_name', 'unit_type', 'unit_status', 'unit_section_name', 'section_id']
        for campo in campos_string:
            df[campo] = df[campo].astype(str)

        return df
    
    def insert_into_bigquery(self, df, table_name):
        if df.empty:
            print("DataFrame está vazio. Nenhum dado para inserir.")
            return

        df = self.preparar_dados_para_insercao(df)
        table_id = f"{self.bq_project_name}.{self.dataset_name}.user_progress"

        try:
            job = self.bq_client.load_table_from_dataframe(df, table_id)
            job.result()
            print(f"Inseridos {len(df)} registros na tabela user_progress.")
        except Exception as e:
            print(f"Erro ao inserir dados no BigQuery para a tabela user_progress: {str(e)}")
            traceback.print_exc()

    def is_table_empty(self) -> bool:
        try:
            query = f"SELECT COUNT(*) as row_count FROM `{self.bq_project_name}.{self.dataset_name}.user_progress`"
            query_job = self.bq_client.query(query)
            result = query_job.result()

            row_count = next(result)['row_count']
            return row_count == 0
        except StopIteration:
            return True
        except Exception as e:
            print(f"Erro ao verificar se a tabela user_progress está vazia: {str(e)}")
            traceback.print_exc()
            return True
