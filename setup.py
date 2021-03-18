from os.path import dirname, join

from setuptools import find_packages, setup

from channels_postgres import __version__


readme = open(join(dirname(__file__), 'README.md')).read()

crypto_requires = ['cryptography>=1.3.0']

test_requires = crypto_requires + [
    'pytest',
    'pytest-asyncio',
    'async_generator',
    'async-timeout',
]


setup(
    name='channels_postgres',
    version=__version__,
    url='http://github.com/django/channels_redis/',
    author='Daniel Osaetin',
    author_email='f805nqs6j@relay.firefox.com',
    description='PostgreSQL-backed ASGI channel layer implementation',
    long_description=readme,
    license='BSD',
    zip_safe=False,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    python_requires='>=3.6',
    install_requires=[
        'aioredis~=1.0',
        'msgpack~=1.0',
        'asgiref>=3.2.10,<4',
        'channels<4',
        'aiopg>=1.1.0'
    ],
    extras_require={'cryptography': crypto_requires, 'tests': test_requires},
)