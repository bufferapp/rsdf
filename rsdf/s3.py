import boto3
from io import StringIO
from io import BytesIO
from gzip import GzipFile

s3 = boto3.client('s3')


def gzip_stringio(input_object):
    compressed_object = BytesIO()

    input_object.seek(0)

    with GzipFile(fileobj=compressed_object, mode='wb') as gzf:
        gzf.write(input_object.getvalue().encode('utf-8'))

    compressed_object.seek(0)

    return compressed_object


def to_s3(self, bucket, key, compress=True, index=False,
          header=None, **kwargs):
    df_csv_object = StringIO()

    self.to_csv(df_csv_object, index=index, header=header, **kwargs)

    if compress:
        df_csv_object = gzip_stringio(df_csv_object)

    return s3.upload_fileobj(df_csv_object, bucket, key)
