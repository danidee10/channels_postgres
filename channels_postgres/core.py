import sys
import uuid
import asyncio
import logging

from django.db.backends.postgresql.base import DatabaseWrapper

import aiopg
import msgpack
from channels.layers import BaseChannelLayer

from .db import DatabaseLayer


if sys.version_info < (3, 7):
    create_task = asyncio.ensure_future
else:
    from asyncio import create_task

# Create two connection pools
# short_pool is for short lived connections (i.e for sending, flushing and discarding)
logger = logging.getLogger(__name__)
pool = None
short_pool = None
creating_pool = False


class PostgresChannelLayer(BaseChannelLayer):
    """
    Postgres channel layer.

    It uses the NOTIFY/LISTEN functionality to broadcast messages
    It uses a mixture of `standard` synchronous database calls and
    asynchronous database calls with aiopg (For receiving messages)

    It also makes use of an internal message table to overcome the
    8000bytes limit of Postgres' NOTIFY messages.
    Which is a far cry from the channels standard of 1MB
    This table has a trigger that sends out the NOTIFY signal.
    """

    def __init__(
        self, prefix='asgi', expiry=60, group_expiry=0,
        symmetric_encryption_keys=None, config=dict(),
        **kwargs
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
        self.django_db = DatabaseLayer(logger=logger)

    async def get_pool(self):
        global pool, short_pool, creating_pool
        if creating_pool and (pool is None or short_pool is None):
            await asyncio.sleep(0.1)
            return await self.get_pool()

        if pool is None or short_pool is None:
            creating_pool = True
            pool = await aiopg.create_pool(**self.db_params, **self.async_lib_config)
            short_pool = await aiopg.create_pool(**self.db_params, maxsize=5)
            creating_pool = False

        return pool, short_pool

    def _setup_encryption(self, symmetric_encryption_keys):
        # See if we can do encryption if they asked
        if symmetric_encryption_keys:
            if isinstance(symmetric_encryption_keys, (str, bytes)):
                raise ValueError(
                    "symmetric_encryption_keys must be a list of possible keys"
                )
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError(
                    'Cannot run with encryption without `cryptography` '
                    'installed.'
                )
            sub_fernets = [
                self.make_fernet(key) for key in symmetric_encryption_keys
            ]
            self.crypter = MultiFernet(sub_fernets)
        else:
            self.crypter = None

    """ Channel layer API """

    extensions = ['groups', 'flush']

    async def send(self, channel, message):
        """Send a message onto a (general or specific) channel."""
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # Make sure the message does not contain reserved keys
        assert "__asgi_channel__" not in message
        message = self.serialize(message)

        _, pool = await self.get_pool()
        with await pool as conn:
            await self.django_db.send_to_channel(
                conn, '', message, self.expiry, channel=channel
            )
            conn.close()

    async def _get_message_from_channel(self, channel):
        retrieve_events_sql = f'LISTEN "{channel}";'
        retrieve_queued_messages_sql = """
                DELETE FROM channels_postgres_message
                WHERE id = (
                    SELECT id
                    FROM channels_postgres_message
                    WHERE channel=%s AND expire > NOW()
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    )
                RETURNING message;
            """

        pool, _ = await self.get_pool()
        with await pool as conn:
            cur = await conn.cursor()
            await cur.execute(retrieve_queued_messages_sql, (channel,))
            message = await cur.fetchone()

            if not message:
                # Unlisten and clear pending messages (From other connections) from the queue
                await cur.execute('UNLISTEN *;')
                for _ in range(conn.notifies.qsize()):
                    conn.notifies.get_nowait()

                await cur.execute(retrieve_events_sql)
                event = await conn.notifies.get()
                message_id = event.payload

                retrieve_message_sql = (
                    'DELETE FROM channels_postgres_message '
                    'WHERE id=%s RETURNING message;'
                )
                await cur.execute(retrieve_message_sql, (message_id,))
                message = await cur.fetchone()

            message = self.deserialize(message[0])

            return message

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine waits on the same channel, the first waiter
        will be given the message when it arrives.
        """
        assert self.valid_channel_name(channel)
        if '!' in channel:
            real_channel = self.non_local_name(channel)
            assert real_channel.endswith(
                self.client_prefix + '!'
            ), 'Wrong client prefix'

        return await self._get_message_from_channel(channel)

    async def new_channel(self, prefix='specific'):
        """
        Returns a new channel name that can be used by something in our
        process as a specific channel.
        """
        return "%s.%s!%s" % (
            prefix,
            self.client_prefix,
            uuid.uuid4().hex,
        )

    """ Flush extension """

    async def flush(self):
        """
        Deletes all messages and groups in the database

        This doesn't flush Postgres' NOTIFY queue
        Postgres doesn't provide access to this table/queue.
        """
        truncate_table_sql = (
            'TRUNCATE channels_postgres_groupchannel; '
            'TRUNCATE channels_postgres_message;'
        )

        _, pool = await self.get_pool()
        with await pool as conn:
            cur = await conn.cursor()
            await cur.execute(truncate_table_sql)

    """Groups extension """

    async def group_add(self, group, channel):
        """Adds the channel name to a group to the Postgres table."""
        group_key = self._group_key(group)

        _, pool = await self.get_pool()
        with await pool as conn:
            await self.django_db.add_channel_to_group(
                conn, group_key, channel, self.group_expiry
            )
            conn.close()

    async def group_discard(self, group, channel, expire=None):
        """
        Removes the channel from the named group if it is in the group;
        does nothing otherwise (does not error)
        """
        assert self.valid_group_name(group), 'Group name not valid'
        assert self.valid_channel_name(channel), 'Channel name not valid'
        group_key = self._group_key(group)

        delete_channel_sql = (
            'DELETE FROM channels_postgres_groupchannel '
            'WHERE group_key=%s AND channel =%s'
        )
        _, pool = await self.get_pool()
        with await pool as conn:
            cur = await conn.cursor()
            await cur.execute(delete_channel_sql, (group_key, channel))

            conn.close()

        # Delete expired groups (if enabled) and messages
        if self.group_expiry > 0:
            create_task(self.django_db.delete_expired_groups(self.db_params))

        create_task(self.django_db.delete_expired_messages(self.db_params, expire))

    async def group_send(self, group, message):
        """Sends a message to the entire group."""
        assert self.valid_group_name(group), "Group name not valid"

        group_key = self._group_key(group)
        # Retrieve list of all channel names related to the group
        message = self.serialize(message)

        _, pool = await self.get_pool()
        with await pool as conn:
            await self.django_db.send_to_channel(
                conn, group_key, message, self.expiry
            )
            conn.close()

    def _group_key(self, group):
        """Common function to make the storage key for the group."""
        return ("%s:group:%s" % (self.prefix, group))

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
