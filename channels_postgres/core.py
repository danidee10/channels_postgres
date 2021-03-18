import uuid
import asyncio
import logging

from django.db.backends.postgresql.base import DatabaseWrapper

import aiopg
import msgpack
import psycopg2

from channels.exceptions import ChannelFull
from channels.layers import BaseChannelLayer


logger = logging.getLogger(__name__)


class PostgresChannelLayer(BaseChannelLayer):
    """
    Postgres channel layer.

    It routes all messages into remote Redis server. Support for
    sharding among different Redis installations and message
    encryption are provided.

    serialization/deserialization is handled by msgpack
    """

    def __init__(
        self, prefix='asgi', symmetric_encryption_keys=None, **kwargs
    ):
        self.prefix = prefix
        self.client_prefix = uuid.uuid4().hex[:5]
        self._setup_encryption(symmetric_encryption_keys)
        self._ready = False

        try:
            kwargs['OPTIONS']
        except KeyError:
            kwargs['OPTIONS'] = {}

        self.db_wrapper = DatabaseWrapper(kwargs)
        self.db_params = self.db_wrapper.get_connection_params()

        try:
            asyncio.create_task(self._setup_database())
        except RuntimeError:
            # We're running outside an event loop
            loop = asyncio.new_event_loop()
            loop.run_until_complete(self._setup_database())

    async def _setup_database(self):
        """Setup the database tables, triggers and procedures."""
        setup_database_sql = """
            CREATE TABLE IF NOT EXISTS channels_postgres_messages (
                id serial PRIMARY KEY NOT NULL,
                channel varchar(100) NOT NULL,
                message bytea CHECK (length(message) <= 1000000),
                expire timestamp NOT NULL DEFAULT (NOW() + INTERVAL '1 minute')
            );

            CREATE OR REPLACE FUNCTION delete_expired_messages() RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                DELETE FROM channels_postgres_messages WHERE expire < NOW() - INTERVAL '1 minute';
                RETURN NEW;
                END;
                $$;

            DO $$ BEGIN
                CREATE TRIGGER delete_expired_messages_trigger
                    AFTER INSERT ON channels_postgres_messages
                    EXECUTE PROCEDURE delete_expired_messages();
                EXCEPTION
                    WHEN others THEN null;
            END $$;

            CREATE OR REPLACE FUNCTION channels_postgres_notify()
                RETURNS trigger AS $$
                DECLARE
                BEGIN
                PERFORM pg_notify(NEW.channel, NEW.id::text);
                RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

            DO $$ BEGIN
                CREATE TRIGGER channels_postgres_notify_trigger
                    AFTER INSERT ON channels_postgres_messages
                    FOR EACH ROW
                    EXECUTE PROCEDURE channels_postgres_notify();
                EXCEPTION
                    WHEN others THEN null;
            END $$;
        """
        self.pool = await aiopg.create_pool(**self.db_params)
        with await self.pool.cursor() as cur:
            await cur.execute(setup_database_sql)
            self._ready = True

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

    async def _database_is_ready(self):
        """Check if the database is ready."""
        while True:
            if self._ready:
                break
            print(self._ready, 'Task done!!!!!!!!!!!!@@@@@@@@')
            await asyncio.sleep(1)

    """ Channel layer API """

    extensions = ['groups', 'flush']

    async def _send_on_channel(self, cur, channel, message):
        """Send a message on a channel."""
        insert_message_sql = (
            'INSERT INTO channels_postgres_messages (channel, message) '
            'VALUES (%s, %s)'
        )
        message = self.serialize(message)

        await cur.execute(insert_message_sql, (channel, message))
        print('sent for channel:', channel)

    async def send(self, channel, message):
        """Send a message onto a (general or specific) channel."""
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # Make sure the message does not contain reserved keys
        assert "__asgi_channel__" not in message
        # If it's a process-local channel,
        # strip off local part and stick full name in message
        # channel_non_local_name = channel
        if "!" in channel:
            message = dict(message.items())
            message["__asgi_channel__"] = channel
            # channel_non_local_name = self.non_local_name(channel)
        # Write out message into expiring key (avoids big items in list)
        # channel_key = self.prefix + channel_non_local_name

        with await self.pool.cursor() as cur:
            await self._send_on_channel(cur, channel, message)

    async def _get_message_from_channel(self, channel):
        retrieve_events_sql = f'LISTEN "{channel}";'
        with await self.pool as conn:
            cur = await conn.cursor()
            await cur.execute(retrieve_events_sql)
            print(f'listening on channel {channel}')

            event = await conn.notifies.get()
            message_id = event.payload
            print('Receive <-', event.payload)

            retrieve_message_sql = (
                'SELECT message from channels_postgres_messages '
                'WHERE id=%s'
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

        await self._database_is_ready()

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

        NOTE: This doesn't flush Postgres' NOTIFY queue
        Postgres doesn't provide access to this table/queue.
        """
        truncate_table_sql = (
            'TRUNCATE channels_postgres; '
            'TRUNCATE channels_postgres_messages;'
        )

        with await self.pool.cursor() as cur:
            await cur.execute(truncate_table_sql)

    """ Groups extension """

    async def group_add(self, group, channel):
        """
        Adds the channel name to a group to the Postgres table.

        TODO: Investigate group expiry option
        """
        create_table_sql = """
                CREATE TABLE IF NOT EXISTS channels_postgres (
                    id serial PRIMARY KEY NOT NULL,
                    group_key varchar(100) NOT NULL,
                    channel varchar(100) NOT NULL
                )
            """
        group_key = self._group_key(group)
        group_add_sql = (
            'INSERT INTO channels_postgres (group_key, channel) '
            'VALUES (%s, %s)'
        )

        await self._database_is_ready()

        self.pool._loop = asyncio.get_event_loop()

        with await self.pool.cursor() as cur:
            await cur.execute(create_table_sql)
            await cur.execute(group_add_sql, (group_key, channel))

            print(f'Channel {channel} added to Group {group_key}')

    async def group_discard(self, group, channel):
        """
        Removes the channel from the named group if it is in the group;
        does nothing otherwise (does not error)
        """
        assert self.valid_group_name(group), 'Group name not valid'
        assert self.valid_channel_name(channel), 'Channel name not valid'
        group_key = self._group_key(group)

        delete_channel_sql = (
            'DELETE FROM channels_postgres '
            'WHERE group_key=%s AND channel =%s'
        )
        with await self.pool.cursor() as cur:
            await cur.execute(delete_channel_sql, (group_key, channel))

    async def group_send(self, group, message):
        """Sends a message to the entire group."""
        assert self.valid_group_name(group), "Group name not valid"
        # Retrieve list of all channel names
        group_key = self._group_key(group)
        retrieve_channels_sql = (
            'SELECT DISTINCT group_key,channel '
            'FROM channels_postgres WHERE group_key=%s;'
        )
        await self._database_is_ready()

        pool = await aiopg.create_pool(**self.db_params)
        with await pool.cursor() as cur:
            await cur.execute(retrieve_channels_sql, (group_key,))

            channels = []
            async for row in cur:
                channels.append(row[1])

            for channel in channels:
                await self._send_on_channel(cur, channel, message)

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
