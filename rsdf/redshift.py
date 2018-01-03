import os
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
from pandas.io.sql import SQLTable, pandasSQL_builder
from . import s3
import tempfile
import numpy as np
import json
import pandas as pd
from sqlalchemy import text
from smart_open import smart_open
from datetime import datetime


def get_engine():
    redshift_db_name = os.getenv('REDSHIFT_DB_NAME')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')
    redshift_endpoint = os.getenv('REDSHIFT_ENDPOINT')
    redshift_db_port = int(os.getenv('REDSHIFT_DB_PORT'), 0)

    engine_string = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        redshift_user, redshift_password, redshift_endpoint,
        redshift_db_port, redshift_db_name)

    return create_engine(engine_string)

def get_dataframe(query):
    engine = get_engine()
    df = pd.read_sql_query(query, engine)

    return df

def prepare_dataframe_for_loading(dataframe):
    # do some cleanup
    #   - escape newlines
    #   - convert all dicts and lists to json formatted strings
    #   - remove | characters
    dataframe = dataframe.copy(deep=True)
    object_types = get_dataframe_column_object_types(dataframe)
    for c in object_types:
        dataframe[c] = dataframe[c].fillna('')
        dataframe[c] = dataframe[c].map(lambda o: o.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if isinstance(o, datetime) else o)
        dataframe[c] = dataframe[c].map(lambda o: json.dumps(o) if not isinstance(o, str) else o)
        dataframe[c] = dataframe[c].map(lambda s: s.encode('unicode-escape').decode() if isinstance(s, str) else s)
        dataframe[c] = dataframe[c].str.replace('\|', '')
    return dataframe


def create_copy_statement(tablename, schemaname, columns, s3_bucket_url, credentials):
    if schemaname == "" or schemaname is None:
        full_table_name = tablename
    else:
        full_table_name = "{schemaname}.{tablename}".format(**locals())

    stmt = ("copy {full_table_name} {columns} from '{s3_bucket_url}' "
            "credentials '{credentials}' "
            "EMPTYASNULL "
            "BZIP2 "
            "DELIMITER '|';".format(**locals()))
    return stmt


def load_dataframe(dataframe, tablename, schemaname='public', columns=None, exists='fail', **args):
    table = get_sa_table_for_dataframe(dataframe, tablename, schemaname)

    dataframe = prepare_dataframe_for_loading(dataframe)
    s3_url = '{0}/{0}.csv.bz2'.format(tablename)

    # first compress locally and then stream to s3
    tfile = tempfile.NamedTemporaryFile(suffix='.bz2')
    dataframe.to_csv(tfile.name, header=False, index=False, sep='|', compression='bz2', na_rep='')

    with smart_open(tfile) as tout:
        with s3.open(s3_url, 'wb') as fout:
            fout.write(tout.read())

    if columns is None:
        columns = ''
    else:
        columns = '()'.format(','.join(columns))
    credentials = 'aws_access_key_id={s3_access_key};aws_secret_access_key={s3_secret_key}'.format(**s3.env)
    s3_bucket_url = 's3://buffer-data/{0}'.format(s3_url)

    if table.exists():
        if exists == 'fail':
            raise ValueError("Table Exists")
        elif exists == 'append':
            queue = [create_copy_statement(tablename, schemaname, columns, s3_bucket_url, credentials)]
        elif exists == 'replace':
            queue = [
                'drop table {schemaname}.{tablename}'.format(**locals()),
                table.sql_schema(),
                create_copy_statement(tablename, schemaname, columns, s3_bucket_url, credentials)
            ]
        elif exists == 'update':
            primary_key = args.get('primary_key')
            staging_table = '#{tablename}'.format(**locals())
            if not primary_key:
                raise ValueError("Expected a primary_key argument, since exists='update'")
            if isinstance(primary_key, str):
                primary_key = [primary_key]

            primary_key_hash_clause = 'md5({0})'.format(' || '.join(k for k in primary_key))
            queue = [
                'drop table if exists {staging_table}'.format(**locals()),
                'create table {staging_table} (like {schemaname}.{tablename})'.format(**locals()),
                create_copy_statement(staging_table, '', columns, s3_bucket_url, credentials),
                'delete from {schemaname}.{tablename} where {primary_key_hash_clause} in (select {primary_key_hash_clause} from {staging_table})'.format(**locals()),
                'insert into {schemaname}.{tablename} (select * from {staging_table})'.format(**locals())
            ]
        else:
            raise ValueError("Bad option for `exists`")
    else:
        queue = [table.sql_schema(), create_copy_statement(tablename, schemaname, columns, s3_bucket_url, credentials)]

    sa_engine = get_engine()
    with sa_engine.begin() as con:
        for stmt in queue:
            try:
                con.execute(stmt)
            except Exception as e:
                print('Error executing {}...'.format(stmt[:10]))


def get_table_ddl(table_name, schema='public'):
    query = text("""
        select ddl
            from(
                select *
                from admin.v_generate_tbl_ddl
                where schemaname = :schema and tablename= :table
            )
            order by seq
    """)
    engine = get_engine()
    r = engine.execute(query, table=table_name, schema=schema)
    lines = [row[0] for row in r]
    return ('\n'.join(lines))


def get_dataframe_column_object_types(dataframe):
    df_grouped_types = dataframe.columns.to_series().groupby(dataframe.dtypes).groups
    object_types = df_grouped_types.get(np.dtype('O'))
    object_types = object_types if object_types.any() else []
    return object_types


def get_sa_table_for_dataframe(dataframe, tablename, schemaname):
    sa_engine = get_engine()
    # get max lengths for strings and use it to set dtypes
    dtypes = {}
    object_types = get_dataframe_column_object_types(dataframe)

    for c in object_types:
        if dataframe[c].dtype == np.dtype('O'):
            n = dataframe[c].map(lambda c: len(str(c)) if c else None).max()
            # we use 10 times the max length or varchar(max)
            dtypes[c] = VARCHAR(min([n*10, 65535]))

    table = SQLTable(tablename, pandasSQL_builder(sa_engine, schema=schemaname),
                     dataframe, if_exists=True, index=False, dtype=dtypes)

    return table

def execute(query):
    engine = get_engine()
    return engine.execute(query)
