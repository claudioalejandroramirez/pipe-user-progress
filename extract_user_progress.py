import pandas as pd
import requests as r
from APIConfig import config

class DataExtractor:
    def __init__(self):
        # A inicialização pode ser expandida conforme necessário
        pass

    def request_to_df(self, api_url, querystring=None, return_raw=False):
        headers = config.header
        response = r.get(api_url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()

        if return_raw:
            return data
        return pd.json_normalize(data)

    def dict_to_columns(self, df, keys=None, new_names=[], prefix=True):
        for col in df.columns:
            if isinstance(df[col].iloc[0], dict):
                keys_to_use = keys if keys != 'all' else df[col].iloc[0].keys()
                new_names = keys_to_use if not new_names else new_names

                if len(keys_to_use) != len(new_names):
                    raise ValueError("O número de chaves e novos nomes precisa ser o mesmo.")

                for key, new_name in zip(keys_to_use, new_names):
                    new_col_name = f"{col}_{new_name}" if prefix else new_name
                    df[new_col_name] = df[col].apply(lambda x: x.get(key, None))
                del df[col]
        return df

    def list_to_columns(self, df, indexes=None, new_names=[], prefix=True):
        for col in df.columns:
            if isinstance(df[col].iloc[0], list):
                if indexes == 'all':
                    df = df.explode(col)
                else:
                    for index, new_name in zip(indexes, new_names):
                        new_col_name = f"{col}_{new_name}" if prefix else new_name
                        df[new_col_name] = df[col].apply(lambda x: x[index] if len(x) > index else None)
                    del df[col]
        return df

    def get_users_ids(self, exclude_tags, items_per_page=200):
        all_ids = []
        page = 1
        while True:
            next_page_url = f'https://academy.contoso.com/admin/api/v2/users?items_per_page={items_per_page}&page={page}'
            response_data = self.request_to_df(next_page_url, return_raw=True)
            user_ids = [
                user['id'] for user in response_data['data']
                if user.get('tags') and not any(tag in exclude_tags for tag in user['tags'])
            ]
            all_ids.extend(user_ids)

            next_page = response_data['meta']['page'] < response_data['meta']['totalPages']
            if not next_page:
                break
            page += 1
        return all_ids

    def fetch_data_batch(self, user_ids):
        all_data = []

        for user_id in user_ids:
            try:
                api_url = f'https://academy.contoso.com/admin/api/v2/users/{user_id}/progress'
                user_data = self.request_to_df(api_url, return_raw=True)['data']

                for course_progress in user_data:
                    for progress_section in course_progress.get('progress_per_section_unit', []):
                        units = progress_section.get('units', [])

                        for unit in units:
                            result_data = {
                                'user_id': user_id,
                                'course_id': course_progress.get('course_id', None),
                                'status': course_progress.get('status', None),
                                'progress_rate': course_progress.get('progress_rate', None),
                                'average_score_rate': course_progress.get('average_score_rate', None),
                                'time_on_course': course_progress.get('time_on_course', None),
                                'total_units': course_progress.get('total_units', None),
                                'completed_units': course_progress.get('completed_units', None),
                                'section_id': progress_section.get('section_id', None), 
                            }

                            for key, value in progress_section.items():
                                if key != 'units':
                                    result_data[key] = value

                            for unit_key, unit_value in unit.items():
                                result_data[unit_key] = unit_value

                            all_data.append(result_data)

            except r.exceptions.HTTPError:
                print(f'O usuário {user_id} não possui registros no banco, pulando...')

        return all_data
