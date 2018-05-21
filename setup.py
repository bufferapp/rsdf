# !/usr/bin/env python

from setuptools import setup

setup(
    name='rsdf',
    packages=['rsdf'],
    version='0.9.0',
    description='Redshift interface to Pandas DataFrames',
    author='Michael Erasmus',
    author_email='michael@buffer.com',
    url='https://github.com/bufferapp/rsdf',
    license='MIT',
    keywords=['redshift', 'pandas', 'upsert'],
    install_requires=[
        'boto3',
        'psycopg2-binary',
        'pandas',
        'sqlalchemy',
        'sqlalchemy-redshift'
    ]
)
