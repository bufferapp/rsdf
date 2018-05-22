import os
from pandas.io.sql import SQLTable, pandasSQL_builder, _engine_builder
from sqlalchemy_redshift.commands import CopyCommand


def generate_redshift_engine_string():
    redshift_db_name = os.getenv('REDSHIFT_DB_NAME')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')
    redshift_endpoint = os.getenv('REDSHIFT_ENDPOINT')
    redshift_db_port = int(os.getenv('REDSHIFT_DB_PORT', 0))

    return "postgres+psycopg2://{}:{}@{}:{}/{}".format(
        redshift_user, redshift_password, redshift_endpoint,
        redshift_db_port, redshift_db_name
    )


def to_redshift(self, table_name, s3_bucket, s3_key, engine=None, schema=None,
                if_exists='fail', index=False, compress=True, primary_key=None,
                aws_access_key_id=None, aws_secret_access_key=None, **kwargs):

    if not engine:
        engine = generate_redshift_engine_string()

    if not aws_access_key_id:
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    if not aws_secret_access_key:
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    # Get Pandas SQLTable object
    table = SQLTable(table_name, pandasSQL_builder(engine, schema=schema),
                     self, if_exists=if_exists, schema=schema, index=index)

    # Full table name with schema
    if schema:
        full_table_name = str(schema) + '.' + str(table_name)
    else:
        full_table_name = str(table_name)

    # Check table
    if table.exists():
        if if_exists == 'fail':
            raise ValueError("Table {} already exists.".format(table_name))
        elif if_exists == 'append':
            queue = [CopyCommand(to=table, data_location='s3://{}/{}'.format(s3_bucket, s3_key), access_key_id=aws_access_key_id,
                                 secret_access_key=aws_secret_access_key, format='CSV', compression='GZIP' if compress else None)]
        elif if_exists == 'replace':
            queue = [
                'drop table {};'.format(full_table_name),
                table.sql_schema() + ";",
                CopyCommand(to=table, data_location='s3://{}/{}'.format(s3_bucket, s3_key), access_key_id=aws_access_key_id,
                            secret_access_key=aws_secret_access_key, format='CSV', compression='GZIP' if compress else None)
            ]
        elif if_exists == 'update':
            staging_table = '{}_staging'.format(table_name)

            if not primary_key:
                raise ValueError(
                    "Expected a primary key to update existing table")

            queue = [
                'begin;',
                'drop table if exists {};'.format(staging_table),
                'create temporary table {} (like {});'.format(
                    staging_table, full_table_name),
                CopyCommand(to=table, data_location='s3://{}/{}'.format(s3_bucket, s3_key), access_key_id=aws_access_key_id,
                            secret_access_key=aws_secret_access_key, format='CSV', compression='GZIP' if compress else None),
                'delete from {full_table_name} where {primary_key} in (select {primary_key} from {staging_table});'.format(
                    full_table_name=full_table_name, primary_key=primary_key, staging_table=staging_table),
                'insert into {} (select * from {});'.format(full_table_name,
                                                            staging_table),
                'end;'
            ]
        else:
            raise ValueError("{} is not valid for if_exists".format(if_exists))
    else:
        queue = [table.sql_schema() + ";",
                 CopyCommand(to=table, data_location='s3://{}/{}'.format(s3_bucket, s3_key), access_key_id=aws_access_key_id,
                             secret_access_key=aws_secret_access_key, format='CSV', compression='GZIP' if compress else None)]

    # Save DataFrame to S3
    self.to_s3(bucket=s3_bucket, key=s3_key, index=index, compress=compress)

    # Execute queued statements
    engine = _engine_builder(engine)
    with engine.begin() as con:
        for stmt in queue:
            con.execute(stmt)
