"""common db methods."""

import asyncio
import logging
import random
import typing
from datetime import UTC, datetime, timedelta

import psycopg
import psycopg_pool
import psycopg_pool.base

from .models import GroupChannel, Message

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


class DatabaseLayer:
    """
    Encapsulates database operations

    Basic database operations are either handled directly via Django's ORM.
    Complex queries that are not supported by Django's ORM are handled
    via raw SQL queries via psycopg.
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
        retrieve_channels_sql = (
            'SELECT DISTINCT group_key,channel '
            'FROM channels_postgres_groupchannel WHERE group_key=%s;'
        )
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
        message_add_sql = (
            'INSERT INTO channels_postgres_message (channel, message, expire) VALUES (%s, %s, %s)'
        )

        if channel is None:
            channels = await self.retrieve_group_channels(group_key)
            if not channels:
                self.logger.warning('Group: %s does not exist, did you call group_add?', group_key)
                return
        else:
            channels = [channel]

        expiry_datetime = datetime.now(UTC) + timedelta(seconds=expire)
        # Bulk insert messages
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                data = [(channel, message, expiry_datetime) for channel in channels]
                await cursor.executemany(message_add_sql, data)

    async def add_channel_to_group(self, group_key: str, channel: str, expire: int) -> None:
        """Adds a channel to a group"""
        expiry_datetime = datetime.now(UTC) + timedelta(seconds=expire)
        group_add_sql = (
            'INSERT INTO channels_postgres_groupchannel (group_key, channel, expire) '
            'VALUES (%s, %s, %s)'
        )

        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                data = (group_key, channel, expiry_datetime)
                await cursor.execute(group_add_sql, data)

        self.logger.debug('Channel %s added to Group %s', channel, group_key)

    async def delete_expired_groups(self) -> None:
        """Deletes expired groups after a random delay"""
        now = datetime.now(UTC)
        delete_expired_groups_sql = 'DELETE FROM channels_postgres_groupchannel WHERE expire < %s'
        expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired groups in %s seconds...', expire)

        await asyncio.sleep(expire)

        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(delete_expired_groups_sql, (now,))

    async def delete_expired_messages(self, expire: typing.Optional[int] = None) -> None:
        """Deletes expired messages after a set time or random delay"""
        now = datetime.now(UTC)
        delete_expired_messages_sql = 'DELETE FROM channels_postgres_message WHERE expire < %s'
        if expire is None:
            expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired messages in %s seconds...', expire)

        await asyncio.sleep(expire)

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
        now = datetime.now(UTC)
        retrieve_queued_messages_sql = """
            DELETE FROM channels_postgres_message
            WHERE id IN (
                SELECT id
                FROM channels_postgres_message
                WHERE expire > %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id::text, channel, message, extract(epoch from expire)::text
        """
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(retrieve_queued_messages_sql, (now,))

                return await cursor.fetchall()

    async def retrieve_non_expired_queued_message_from_channel(
        self, channel: str
    ) -> typing.Optional[tuple[bytes]]:
        """Retrieves a non-expired message from a channel"""
        now = datetime.now(UTC)
        retrieve_queued_messages_sql = """
            DELETE FROM channels_postgres_message
            WHERE id = (
                SELECT id
                FROM channels_postgres_message
                WHERE channel=%s AND expire > %s
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                )
            RETURNING message;
        """
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(retrieve_queued_messages_sql, (channel, now))
                message = await cursor.fetchone()

                return typing.cast(typing.Optional[tuple[bytes]], message)

    async def delete_message_returning_message(
        self, message_id: int
    ) -> typing.Optional[tuple[bytes]]:
        """Deletes a message from the database and returns the message"""
        delete_message_returning_message_sql = (
            'DELETE FROM channels_postgres_message WHERE id=%s RETURNING message;'
        )

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
            await conn.execute(f'TRUNCATE TABLE {Message._meta.db_table}')  # pylint: disable=W0212
            await conn.execute(f'TRUNCATE TABLE {GroupChannel._meta.db_table}')  # pylint: disable=W0212
