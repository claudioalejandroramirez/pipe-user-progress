class APIConfig:
    def __init__(self, bearer_token='', content_type='application/json', client_id='', lw_client=''):
        self.bearer_token = bearer_token
        self.content_type = content_type
        self.client_id = client_id
        self.lw_client = lw_client
        self.header = self.build_header()

    def build_header(self):
        return {
            'Authorization': f'Bearer {self.bearer_token}',
            'Content-Type': self.content_type,
            'Lw-Client': self.lw_client
        }

# Configuração manual dos valores
config = APIConfig(
    bearer_token='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    client_id='60b14bxxxxxxxxxxxxxxxxxxxx2',
    lw_client='60xxxxxxxxxxxxxxxxxxxxxxxxxx'
)
