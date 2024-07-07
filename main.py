import extract_user_progress
from DataInjector import DataInjector
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import time


def main(event, context):
    print("Iniciando o processo de extração de progresso do usuário...")

    extractor_user_progress = extract_user_progress.DataExtractor()

    exclude_tags = [
        'MKTPLACE', 'CURSOLIVRE', 'investigar_progress', 'teste-teste-teste',
        'b2b-serpro', 'INVESTPRO', 'ALUNO-USP', 'case-esg', 'b2b-taginvest',
        'SalaryFits', 'MATRIZ-A', 'DATA-TEAM', 'MWM-B2B', 'fix-cripto', 'Tech-Team',
        'Exame', 'Cross-Carreira', 'teste-tag3', 'Programa-Musa-b2b', 'lead-master-metaverso',
        'Respondentes-Crossell2', 'future-dojo', 'JBJUNHO', 'Academy-Team', 'esg-investimentos',
        'B2B-LEADS', 'Cross-ESG', 'Cross-Invest', 'Dantas-Turma-2', 'academy-play', 'usp',
        'Dantas-Turma-02', 'fix-2102', 'b2b-procempa', 'desmatricula', 'BF21-CORTESIA',
        'BF21-CORTESIAS', 'mkt-t4', 'resgate', 'Fix-enroll', 'Cross-EF', 'Future Dojo',
        'conceitos-lideranca', 'Respondentes-Crossell', 'News-esg', 'Dantas-Turma-1',
        'b2b-quanta_previdencia', 'lead-wep8', 'leadmbaesg', 'como-mexer-no-teams',
        'lead-expert-metaverso', 'News-carreira', 'mba-ia-trilha-a-turma-agosto-23-carol-teste-1',
        'tutora', 'Data-Team', 'mba-ia-trilha-b-turma-agosto-23-carol-teste-1', 'lead-rendafiis',
        'mba-inteligencia-artificial-para-negocios-t2'
    ]
    
    schema_bigquery = [
        {"name": "user_id", "type": "STRING"},
        {"name": "course_id", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "progress_rate", "type": "FLOAT"},
        {"name": "average_score_rate", "type": "FLOAT"},
        {"name": "time_on_course", "type": "FLOAT"},
        {"name": "total_units", "type": "STRING"},
        {"name": "completed_units", "type": "FLOAT"},
        {"name": "unit_id", "type": "STRING"},
        {"name": "unit_name", "type": "STRING"},
        {"name": "unit_type", "type": "STRING"},
        {"name": "unit_status", "type": "STRING"},
        {"name": "time_on_unit", "type": "FLOAT"},
        {"name": "score_on_unit", "type": "FLOAT"},
        {"name": "unit_duration", "type": "FLOAT"},
        {"name": "unit_section_name", "type": "STRING"},
        {"name": "unit_progress_rate", "type": "FLOAT"},
        {"name": "section_id", "type": "STRING"}
    ]

    user_ids = extractor_user_progress.get_users_ids(exclude_tags)
    injector = DataInjector()

    start_time = time.time()

    process_user_progress_concurrently(extractor_user_progress, user_ids, injector, schema_bigquery)

    execution_time = time.time() - start_time
    print(f"Tempo de execução: {execution_time} segundos")

def process_user_progress_concurrently(extractor, user_ids, injector, schema_bigquery):
    print("Extraindo progresso do usuário...")
    max_workers = 70
    batch_size = 30
    user_id_batches = [user_ids[i:i + batch_size] for i in range(0, len(user_ids), batch_size)]
    all_user_progress_data = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(extractor.fetch_data_batch, batch) for batch in user_id_batches]
        for future in as_completed(futures):
            all_user_progress_data.extend(future.result())

    if all_user_progress_data:
        df_user_progress = pd.DataFrame(all_user_progress_data)
        df_ajustado = ajustar_schema_dataframe(df_user_progress, schema_bigquery)
        chunk_and_insert_data(df_ajustado, injector, 'user_progress')

def chunk_and_insert_data(df, injector, table_name):
    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    for df_chunk in chunker(df, 15000):
        injector.insert_into_bigquery(df_chunk, table_name)

def ajustar_schema_dataframe(df, schema_bigquery):
    colunas_schema = [campo['name'] for campo in schema_bigquery]
    colunas_df = df.columns.tolist()
    
    for coluna in colunas_df:
        if coluna not in colunas_schema:
            df = df.drop(coluna, axis=1)
    
    for campo in schema_bigquery:
        if campo['name'] not in colunas_df:
            tipo_pandas = "float64" if campo['type'] == "FLOAT" else "object"
            df[campo['name']] = pd.Series(dtype=tipo_pandas)
    
    return df

if __name__ == "__main__":
    main(None, None)
