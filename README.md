# channels_postgres

[![Tests](https://github.com/danidee10/channels_postgres/actions/workflows/tests.yml/badge.svg)](https://github.com/danidee10/channels_postgres/actions/workflows/tests.yml) [![channels_postgres pypi](https://img.shields.io/pypi/v/channels_postgres.svg)](https://pypi.python.org/pypi/channels_postgres)


A Django Channels channel layer that uses PostgreSQL as its backing store

## Installation

```bash
pip install channels_postgres
```

### Update INSTALLED_APPS

```python
INSTALLED_APPS = (
    ...
    'channels',
    'channels_postgres',
    ...
)
```

### Run migrations for internal tables
```bash
python manage.py migrate channels_postgres
```

### Update DATABASES dictionary

Add the 'channels_postgres' entry to the DATABASES dictionary in your Django settings file like so:

```python
DATABASES = {
	'default': {
		...
	},
	'channels_postgres': {
		'ENGINE': 'django.db.backends.postgresql_psycopg2',
		'NAME': 'postgres',
		'USER': 'postgres',
		'PASSWORD': 'password',
		'HOST': '127.0.0.1',
		'PORT': '5432',
	}
}
```

## Usage

Set up the channel layer in your Django settings file like so:

```python
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_postgres.core.PostgresChannelLayer',
        'CONFIG': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'postgres',
            'USER': 'postgres',
            'PASSWORD': 'password',
            'HOST': '127.0.0.1',
            'PORT': '5432',

            'config: {
                ...
            }
        },
    },
}
```

The Config object is exactly the same as the standard config object for Django's PostgreSQL database. See the django documentation for more information.

`config` is a dictionary of parameters to the underlying async postgres library (in this case `aiopg`) This setting can be used to control the database pool size, connection timeout etc. See the [aiopg documentation](https://aiopg.readthedocs.io/en/stable/core.html?highlight=pool#pool) for more information.

A typical use of `config` would be to increase the `maxsize` of the connection pool. The default of 10 might be too low for sites with a decent amount of traffic.

The config parameters are described below:

### prefix

Prefix to add to all database group keys. Defaults to asgi:. In most cases, you don't need to change this setting because it's only used internally.

### expiry

Message expiry in seconds. Defaults to 60. You generally shouldn't need to change this, but you may want to turn it down if you have peaky traffic you wish to drop, or up if you have peaky traffic you want to backlog until you get to it.
group_expiry

### Group expiry

Defaults to 0.

`0 means disabled!` 

Channels will be removed from the group after this amount of time; it's recommended you reduce it for a healthier system that encourages disconnections. This value should not be lower than the relevant timeouts in the interface server (e.g. the --websocket_timeout to daphne).
capacity

### symmetric_encryption_keys

Pass this to enable the optional symmetric encryption mode of the backend. To use it, make sure you have the cryptography package installed, or specify the cryptography extra when you install channels_postgres:

pip install channels_postgres[cryptography]

symmetric_encryption_keys should be a list of strings, with each string being an encryption key. The first key is always used for encryption; all are considered for decryption, so you can rotate keys without downtime - just add a new key at the start and move the old one down, then remove the old one after the message expiry time has passed.

Data is encrypted both on the wire and at rest in Postgres, though we advise you also route your Postgres connections over TLS for higher security.

Keys should have at least 32 bytes of entropy - they are passed through the SHA256 hash function before being used as an encryption key. Any string will work, but the shorter the string, the easier the encryption is to break.

If you're using Django, you may also wish to set this to your site's SECRET_KEY setting via the CHANNEL_LAYERS setting:

```python
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_postgres.core.PostgresChannelLayer',
        'CONFIG': {
            ...,
            'symmetric_encryption_keys': [SECRET_KEY],
        },
    },
}
```

## Deviations from the channels spec

### group_expiry

Defaults to 0 (which means disabled). This option is tied too closely to `daphne` (The official ASGI interface server for `django-channels`). It makes no sense if you're using an alternate `ASGI` server (like `Uvicorn`) which doesn't disconnect WebSockets automatically.

Setting it to a non zero value enables the expected behaviour.

### channel_capacity

RDMS' like `PostgreSQL` were specifically built to handle huge amounts of data without crashing down and using too much memory. Hence, there's no channel capacity.

Your database should be able to handle thousands of messages with ease. If you're still worried about the database table growing out of hand, you can reduce the `expiry` time of the individual messages so they will be purged if a consumer cannot process them on time.

## Dependencies

Python >= 3.6 is required for `channels_postgres`
