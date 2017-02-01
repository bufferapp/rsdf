import os
from smart_open import smart_open

creds = {
    's3_access_key': os.environ.get('AWS_ACCESS_KEY_ID'),
    's3_secret_key': os.environ.get('AWS_SECRET_ACCESS_KEY')
}


def get_buffer_data_url(with_creds=False):
    if with_creds:
        return 's3://{s3_access_key}:{s3_secret_key}@buffer-data'.format(**creds)
    else:
        return 's3://buffer-data'


def open_buffer_data(path, mode='rb', **kw):
    base_url = get_buffer_data_url(with_creds=True)

    s3_url = '{base_url}/{path}'.format(**locals())
    return smart_open(s3_url, mode, **kw)
