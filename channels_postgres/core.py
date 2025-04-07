
import uuid
import logging
import sys
import platform
import asyncio
from django.db.backends.postgresql.base import DatabaseWrapper

import psycopg
import msgpack
from channels.layers import BaseChannelLayer

from .db import DatabaseLayer

# ProactorEventLoop is not supported by psycopg3 on windows
# https://www.psycopg.org/psycopg3/docs/advanced/async.html
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(
        asyncio.WindowsSelectorEventLoopPolicy()
    )

from asyncio import create_task  # noqa: F401


logger = logging.getLogger(__name__)


async def asyncnext(ait):
    """
    asyncnext is a helper function that returns the next item from an async iterator.
    It is a backport of the built-in anext function that was added in Python 3.10.
    """
    if sys.version_info >= (3, 10):
        return await anext(ait)  # noqa: F821

    return await ait.__anext__()


class PostgresChannelLayer(BaseChannelLayer):
    """
    Postgres channel layer.

    It uses the NOTIFY/LISTEN functionality to broadcast messages
    It uses a mixture of `standard` synchronous database calls and
    asynchronous database calls with aiopg (For receiving messages)

    It also makes use of an internal message table to overcome the
    8000bytes limit of Postgres' NOTIFY messages.
    Which is a far cry from the channels standard of 1MB
    This table has a trigger that sends out the `NOTIFY` signal.
    """

    def __init__(
        self,
        prefix='asgi',
        expiry=60,
        group_expiry=0,
        symmetric_encryption_keys=None,
        config=dict(),
        **kwargs,
    ):
        self.prefix = prefix
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.client_prefix = uuid.uuid4().hex[:5]
        self._setup_encryption(symmetric_encryption_keys)

        self.async_lib_config = config

        try:
            kwargs['OPTIONS']
        except KeyError:
            kwargs['OPTIONS'] = {}

        db_wrapper = DatabaseWrapper(kwargs)
        self.db_params = db_wrapper.get_connection_params()
        self.db_params.pop('cursor_factory')

        self.django_db = DatabaseLayer(self.db_params, logger=logger)

    def _setup_encryption(self, symmetric_encryption_keys):
        # See if we can do encryption if they asked
        if symmetric_encryption_keys:
            if isinstance(symmetric_encryption_keys, (str, bytes)):
                raise ValueError('symmetric_encryption_keys must be a list of possible keys')
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError('Cannot run with encryption without `cryptography` installed.')
            sub_fernets = [self.make_fernet(key) for key in symmetric_encryption_keys]
            self.crypter = MultiFernet(sub_fernets)
        else:
            self.crypter = None

    """ Channel layer API """

    extensions = ['groups', 'flush']

    async def send(self, channel, message):
        """Send a message onto a (general or specific) channel."""
        # Typecheck
        assert isinstance(message, dict), 'message is not a dict'
        try:
            assert self.require_valid_channel_name(channel), 'Channel name not valid'
        except AttributeError:
            assert self.valid_channel_name(channel), 'Channel name not valid'
        # Make sure the message does not contain reserved keys
        assert '__asgi_channel__' not in message
        message = self.serialize(message)

        await self.django_db.send_to_channel('', message, self.expiry, channel=channel)

    async def _get_message_from_channel(self, channel):
        retrieve_events_sql = f'LISTEN "{channel}";'

        async with await psycopg.AsyncConnection.connect(**self.db_params, autocommit=True) as conn:
            message = await self.django_db.retrieve_queued_message_from_channel(conn, channel)
            if not message:
                await conn.execute(retrieve_events_sql)

                event = await asyncnext(conn.notifies())
                message_id = event.payload
                message = await self.django_db.delete_message_returning_message(conn, message_id)

            message = self.deserialize(message[0])

            return message

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine waits on the same channel, the first waiter
        will be given the message when it arrives.
        """
        try:
            assert self.require_valid_channel_name(channel), 'Channel name not valid'
        except AttributeError:
            assert self.valid_channel_name(channel), 'Channel name not valid'
        if '!' in channel:
            real_channel = self.non_local_name(channel)
            assert real_channel.endswith(self.client_prefix + '!'), 'Wrong client prefix'

        await self.django_db.delete_expired_messages(expire=0)

        return await self._get_message_from_channel(channel)

    async def new_channel(self, prefix='specific'):
        """
        Returns a new channel name that can be used by something in our
        process as a specific channel.
        """
        return '%s.%s!%s' % (prefix, self.client_prefix, uuid.uuid4().hex)

    """ Flush extension """

    async def flush(self):
        """
        Unlisten on all channels and
        Deletes all messages and groups in the database
        """
        await self.django_db.flush()

    """Groups extension """

    async def group_add(self, group, channel):
        """Adds the channel name to a group to the Postgres table."""
        group_key = self._group_key(group)

        await self.django_db.add_channel_to_group(group_key, channel, self.group_expiry)

    async def group_discard(self, group, channel, expire=None):
        """
        Removes the channel from the named group if it is in the group;
        does nothing otherwise (does not error)
        """
        try:
            assert self.require_valid_group_name(group), 'Group name not valid'
        except AttributeError:
            assert self.valid_group_name(group), 'Group name not valid'
        try:
            assert self.require_valid_channel_name(channel), 'Channel name not valid'
        except AttributeError:
            assert self.valid_channel_name(channel), 'Channel name not valid'

        group_key = self._group_key(group)

        await self.django_db.delete_channel_group(group_key, channel)

        # Delete expired groups (if enabled) and messages
        if self.group_expiry > 0:
            create_task(self.django_db.delete_expired_groups())

        create_task(self.django_db.delete_expired_messages(expire))

    async def group_send(self, group, message):
        """Sends a message to the entire group."""
        try:
            assert self.require_valid_group_name(group), 'Group name not valid'
        except AttributeError:
            assert self.valid_group_name(group), 'Group name not valid'

        group_key = self._group_key(group)
        # Retrieve list of all channel names related to the group
        message = self.serialize(message)

        await self.django_db.send_to_channel(group_key, message, self.expiry)

    def _group_key(self, group):
        """Common function to make the storage key for the group."""
        return '%s:group:%s' % (self.prefix, group)

    """ Serialization """

    def serialize(self, message):
        """Serializes message to a byte string."""
        value = msgpack.packb(message, use_bin_type=True)
        if self.crypter:
            value = self.crypter.encrypt(value)

        return value

    def deserialize(self, message):
        """Deserializes from a byte string."""
        if self.crypter:
            message = self.crypter.decrypt(message, self.expiry + 10)

        return msgpack.unpackb(message, raw=False)
