from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as f:
    readme = f.read()

with open('LICENSE', encoding='utf-8') as f:
    license = f.read()

setup(
    name='rsdf',
    version='0.1.0',
    description='Set of utils to connect Pandas and Redshift',
    long_description=readme,
    author='Michael Erasmus',
    author_email='michael@buffer.com',
    url='https://github.com/bufferapp/rsdf',
    license=license,
    install_requires=[
        'smart_open',
        'pandas',
        'sqlalchemy',
        'numpy'
    ],
    packages=find_packages(exclude=('tests', 'docs'))
)
