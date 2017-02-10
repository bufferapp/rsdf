import os
from smart_open import smart_open

env = {
    's3_access_key': os.environ.get('AWS_ACCESS_KEY_ID'),
    's3_secret_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),
    's3_root' : os.getenv('REDSHIFT_COPY_S3_ROOT')
}


def get_buffer_data_url(with_creds=False):
    if with_creds:
        return 's3://{s3_access_key}:{s3_secret_key}@{s3_root}'.format(**env)
    else:
        return 's3://{s3_root}'.format(**env)


def open(path, mode='rb', **kw):
    base_url = get_buffer_data_url(with_creds=True)

    s3_url = '{base_url}/{path}'.format(**locals())
    return smart_open(s3_url, mode, **kw)
