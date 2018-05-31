# RSDF

[![Build Status](https://travis-ci.com/bufferapp/rsdf.svg?branch=master)](https://travis-ci.com/bufferapp/rsdf)
[![PyPI version](https://badge.fury.io/py/rsdf.svg)](https://badge.fury.io/py/rsdf)
[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](LICENSE)

Set of utils to connect Pandas DataFrames and Redshift. This module will add a
new function to the `DataFrame` object. Inspired by [josepablog gist](https://gist.github.com/josepablog/1ce154a45dc20348b6718804ac8ad0a5).

## Installation

To install `rsdf`, simply use pip:

```bash
$ pip install rsdf
```

If you were using the older version, you can also install it with `pip`:

```bash
$ pip install git+git://github.com/bufferapp/rsdf.git@d1a5feca220cef9ba7da16da57a746dfb24ee8d7
```

## Usage

Once `rdsf` is imported, the `DataFrame` objects will have new functions:

```python
import pandas as pd
import rsdf


engine_string = 'redshift://user:password@endpoint:port/db'

users = pd.read_sql_query('select * from users limit 10', engine_string)

users['money'] = users['money'] * 42

# Write it back to Redshift
users.to_redshift(
    table_name='users',
    schema='public',
    engine=engine_string,
    s3_bucket='users-data',
    s3_key='rich_users.gzip',
    if_exists='update',
    primary_key='id'
)
```

Alternatively, if no `engine` is provided, the `rsdf` module will try to figure out the engine string from the following environment variables:

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
