# RSDF

Set of utils to connect Pandas' DataFrames and Redshift.

## Installation

`pip install git+git://github.com/bufferapp/rsdf`

## Usage

Make sure you have the next variables in your environment:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `REDSHIFT_USER`
- `REDSHIFT_PASSWORD`
- `REDSHIFT_ENDPOINT`
- `REDSHIFT_DB_NAME`
- `REDSHIFT_DB_PORT`
- `REDSHIFT_COPY_S3_ROOT # Root s3 bucket name for loading data into Redshift`

Run query and get a dataframe
```python
from rsdf import redshift
df = redshift.get_dataframe("select * from events")
```

Or do it manually
```python
# Get PostgreSQL engine (compatible with AWS Redshift)
engine = redshift.get_engine()
df = pd.read_sql_query("select * from events", engine)
```
Load DataFrame into a Redshift table
```python
redshift.load_dataframe(df, 'events', exists='update', primary_key = ['id'])
```

## License

MIT © Buffer
