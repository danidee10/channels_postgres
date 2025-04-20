"""common db methods."""

import asyncio
import hashlib
import logging
import random
import typing
from datetime import datetime, timedelta

import psycopg
import psycopg_pool
import psycopg_pool.base
from psycopg import sql

from .models import GroupChannel, Message

try:
    from datetime import UTC
except ImportError:
    UTC = None  # type: ignore


if typing.TYPE_CHECKING:
    from logging import Logger


# Enable pool logging
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
# logging.getLogger('psycopg.pool').setLevel(logging.DEBUG)


# A global variable is used to ensure only one connection pool is created
# regardless of the amount of threads
# And also to prevent RuntimeErrors when the event loop is closed after running tests
# and psycopg Async workers are not cleaned up properly
is_creating_connection_pool = asyncio.Lock()
connection_pool: typing.Optional[psycopg_pool.AsyncConnectionPool] = None

MESSAGE_TABLE = Message._meta.db_table
GROUP_CHANNEL_TABLE = GroupChannel._meta.db_table


def utc_now() -> datetime:
    """
    Return the current datetime in UTC

    It is compatible with python<=3.10 and python>=3.11
    """
    if UTC:
        return datetime.now(UTC)

    return datetime.utcnow()


class DatabaseLayer:
    """
    Encapsulates database operations

    A connection pool is used for efficient management of database operations
    This is also the reason why psycopg is used directly instead of django's ORM
    which doesn't support connection pooling
    """

    def __init__(
        self,
        psycopg_options: dict[str, typing.Any],
        db_params: dict[str, typing.Any],
        using: str = 'channels_postgres',
        logger: 'Logger' = logging.getLogger('channels_postgres.database'),
    ) -> None:
        self.logger = logger
        self.using = using
        self.db_params = db_params
        self.psycopg_options = psycopg_options

    async def get_db_pool(
        self, db_params: dict[str, typing.Any]
    ) -> psycopg_pool.AsyncConnectionPool:
        """
        Returns a connection pool for the database

        Uses a `Lock` to ensure that only one coroutine can create the connection pool
        Others have to wait until the connection pool is created
        """
        global connection_pool  # pylint: disable=W0603

        async def _configure_connection(conn: psycopg.AsyncConnection) -> None:
            await conn.set_autocommit(True)
            conn.prepare_threshold = 0  # All statements should be prepared
            conn.prepared_max = None  # No limit on the number of prepared statements

        async with is_creating_connection_pool:
            if connection_pool is not None:
                self.logger.debug('Pool %s already exists', connection_pool.name)

                pool_stats = connection_pool.get_stats()
                self.logger.debug('Pool stats: %s', pool_stats)

                return connection_pool

            conn_info = psycopg.conninfo.make_conninfo(conninfo='', **db_params)

            connection_pool = psycopg_pool.AsyncConnectionPool(
                conninfo=conn_info,
                open=False,
                configure=_configure_connection,
                **self.psycopg_options,
            )
            await connection_pool.open(wait=True)

            self.logger.debug('Pool %s created', connection_pool.name)

            return connection_pool

    async def retrieve_group_channels(self, group_key: str) -> list[str]:
        """Retrieves all channels for a group"""
        retrieve_channels_sql = sql.SQL(
            'SELECT DISTINCT group_key,channel FROM {table} WHERE group_key=%s'
        ).format(table=sql.Identifier(GROUP_CHANNEL_TABLE))

        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(retrieve_channels_sql, (group_key,))
                result = await cursor.fetchall()
                return [row[1] for row in result]

    async def send_to_channel(
        self,
        group_key: str,
        message: bytes,
        expire: int,
        channel: typing.Optional[str] = None,
    ) -> None:
        """Send a message to a channel/channels (if no channel is specified)."""
        message_add_sql = sql.SQL(
            'INSERT INTO {table} (channel, message, expire) VALUES (%s, %s, %s)'
        ).format(table=sql.Identifier(MESSAGE_TABLE))

        if channel is None:
            channels = await self.retrieve_group_channels(group_key)
            if not channels:
                self.logger.warning('Group: %s does not exist, did you call group_add?', group_key)
                return
        else:
            channels = [channel]

        expiry_datetime = utc_now() + timedelta(seconds=expire)
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                if len(channels) == 1:
                    # single insert
                    data = (channels[0], message, expiry_datetime)
                    await cursor.execute(message_add_sql, data)
                else:
                    # Bulk insert messages
                    multi_data = [(channel, message, expiry_datetime) for channel in channels]
                    await cursor.executemany(message_add_sql, multi_data)

    async def add_channel_to_group(self, group_key: str, channel: str, expire: int) -> None:
        """Adds a channel to a group"""
        expiry_datetime = utc_now() + timedelta(seconds=expire)
        group_add_sql = sql.SQL(
            'INSERT INTO {table} (group_key, channel, expire) VALUES (%s, %s, %s)'
        ).format(table=sql.Identifier(GROUP_CHANNEL_TABLE))

        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                data = (group_key, channel, expiry_datetime)
                await cursor.execute(group_add_sql, data)

        self.logger.debug('Channel %s added to Group %s', channel, group_key)

    async def delete_expired_groups(self) -> None:
        """Deletes expired groups after a random delay"""
        delete_expired_groups_sql = sql.SQL('DELETE FROM {table} WHERE expire < %s').format(
            table=sql.Identifier(GROUP_CHANNEL_TABLE)
        )

        expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired groups in %s seconds...', expire)
        await asyncio.sleep(expire)

        now = utc_now()
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(delete_expired_groups_sql, (now,))

    async def delete_expired_messages(self, expire: typing.Optional[int] = None) -> None:
        """Deletes expired messages after a set time or random delay"""
        delete_expired_messages_sql = sql.SQL('DELETE FROM {table} WHERE expire < %s').format(
            table=sql.Identifier(MESSAGE_TABLE)
        )

        if expire is None:
            expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired messages in %s seconds...', expire)
        await asyncio.sleep(expire)

        now = utc_now()
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(delete_expired_messages_sql, (now,))

    async def retrieve_non_expired_queued_messages(self) -> list[tuple[str, str, bytes, str]]:
        """
        Retrieves all non-expired messages from the database

        NOTE: Postgres doesn't support ORDER BY for `RETURNING`
        queries. Even if the inner query is ordered, the returning
        clause is not guaranteed to be ordered
        """
        retrieve_queued_messages_sql = sql.SQL(
            """
            DELETE FROM {table}
            WHERE id IN (
                SELECT id
                FROM {table}
                WHERE expire > %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id::text, channel, message, extract(epoch from expire)::text
        """
        ).format(table=sql.Identifier(MESSAGE_TABLE))

        now = utc_now()
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(retrieve_queued_messages_sql, (now,))

                return await cursor.fetchall()

    async def retrieve_non_expired_queued_message_from_channel(
        self, channel: str
    ) -> typing.Optional[tuple[bytes]]:
        """Retrieves a non-expired message from a channel"""
        retrieve_queued_messages_sql = sql.SQL(
            """
            DELETE FROM {table}
            WHERE id = (
                SELECT id
                FROM {table}
                WHERE channel=%s AND expire > %s
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                )
            RETURNING message
        """
        ).format(table=sql.Identifier(MESSAGE_TABLE))

        now = utc_now()
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(retrieve_queued_messages_sql, (channel, now))
                message = await cursor.fetchone()

                return typing.cast(typing.Optional[tuple[bytes]], message)

    def _channel_to_constant_bigint(self, channel: str) -> int:
        """
        Converts a channel name to a constant bigint.
        """
        # Hash the character (SHA-256 gives consistent output)
        hash_bytes = hashlib.sha256(channel.encode('utf-8')).digest()
        # Convert to a large int
        hash_int = int.from_bytes(hash_bytes, byteorder='big')

        # Fit into signed 64-bit bigint range
        signed_bigint = hash_int % (2**64)
        if signed_bigint >= 2**63:
            signed_bigint -= 2**64  # Convert to negative if above max signed

        return signed_bigint

    async def acquire_advisory_lock(self, channel: str) -> bool:
        """Acquires an advisory lock from the database"""
        advisory_lock_id = self._channel_to_constant_bigint(channel)
        acquire_advisory_lock_sql = sql.SQL('SELECT pg_try_advisory_lock(%s::bigint)').format(
            advisory_lock_id=advisory_lock_id
        )

        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(acquire_advisory_lock_sql, (advisory_lock_id,))

                result = await cursor.fetchone()
                return result[0] if result else False

    async def delete_message_returning_message(
        self, message_id: int
    ) -> typing.Optional[tuple[bytes]]:
        """Deletes a message from the database and returns the message"""
        delete_message_returning_message_sql = sql.SQL(
            'DELETE FROM {table} WHERE id=%s RETURNING message'
        ).format(table=sql.Identifier(MESSAGE_TABLE))

        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(delete_message_returning_message_sql, (message_id,))

                return await cursor.fetchone()

    async def delete_channel_group(self, group_key: str, channel: str) -> None:
        """Deletes a channel from a group"""
        await GroupChannel.objects.filter(group_key=group_key, channel=channel).adelete()

    async def flush(self) -> None:
        """
        Flushes the channel layer by truncating the message and group tables
        """
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            await conn.execute(
                sql.SQL('TRUNCATE TABLE {table}').format(table=sql.Identifier(MESSAGE_TABLE))
            )
            await conn.execute(
                sql.SQL('TRUNCATE TABLE {table}').format(table=sql.Identifier(GROUP_CHANNEL_TABLE))
            )
