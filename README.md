# RSDF

Set of utils to connect Pandas DataFrames and Redshift. This module will add a
new function to the `DataFrame` object. Inspired by [josepablog gist](https://gist.github.com/josepablog/1ce154a45dc20348b6718804ac8ad0a5).

## Installation

To install `rsdf`, simply use pip:

```bash
$ pip install rsdf
```

## Usage

Once `rdsf` is imported, the `DataFrame` objects will have new functions:

```python
import pandas as pd
import rsdf


engine_string = 'redshift://user:password@endpoint:port/db'

users = pd.read_sql_query("select * from users limit 10", engine_string)

users['money'] = users['money'] * 42

# Write it back to Redshift
users.to_redshift(
    table_name="users",
    schema='public',
    s3_bucket="users-data",
    s3_key="rich_users.gzip",
    if_exists='update',
    primary_key='id'
)
```

The `rsdf` module will try to figure out the engine string if you don't provide
one. To do that it'll use the following environment variables:

- `REDSHIFT_USER`
- `REDSHIFT_PASSWORD`
- `REDSHIFT_ENDPOINT`
- `REDSHIFT_DB_NAME`
- `REDSHIFT_DB_PORT`

Since `rsdf` uploads the files to S3 and then runs a `COPY` command to add the
data to Redshift you'll also need to provide (or have them in the environment
loaded) these two AWS variables:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

## License

MIT © Buffer
