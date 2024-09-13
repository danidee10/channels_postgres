from os import path

from setuptools import find_packages, setup

from channels_postgres import __version__


crypto_requires = ['cryptography>=1.3.0']

test_requires = crypto_requires + [
    'pytest',
    'pytest-asyncio',
    'async-timeout',
]

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='channels_postgres',
    version=__version__,
    url='http://github.com/danidee10/channels_postgres/',
    author='Daniel Osaetin',
    author_email='f805nqs6j@relay.firefox.com',
    description='PostgreSQL-backed ASGI channel layer implementation',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='BSD',
    zip_safe=False,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    python_requires='>=3.9',
    install_requires=[
        'msgpack~=1.0.7',
        'asgiref>=3.7.2,<4',
        'channels~=4.0.0',
        'aiopg~=1.4.0'
    ],
    extras_require={'cryptography': crypto_requires, 'tests': test_requires},
)
