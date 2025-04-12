"""common db methods."""

import asyncio
import logging
import random
import typing
from dataclasses import dataclass
from datetime import timedelta

import psycopg
import psycopg_pool
from django.utils import timezone

from .models import GroupChannel, Message

if typing.TYPE_CHECKING:
    from logging import Logger


# Enable pool logging
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
# logging.getLogger('psycopg.pool').setLevel(logging.DEBUG)


is_creating_connection_pool = asyncio.Lock()
connection_pool: typing.Optional[psycopg_pool.ConnectionPool] = None


@dataclass
class PsycopgOptions:
    connection_class: typing.Type[psycopg_pool.ConnectionPool] = psycopg_pool.AsyncConnectionPool


class DatabaseLayer:
    """
    Encapsulates database operations

    Basic database operations are either handled directly via Django's ORM.
    Complex queries that are not supported by Django's ORM are handled
    via raw SQL queries via psycopg.
    """

    def __init__(
        self,
        psycopg_options: dict,
        db_params: typing.Mapping[str, typing.Union[str, int]],
        using: str = 'channels_postgres',
        logger: 'Logger' = logging.getLogger('channels_postgres.database'),
    ) -> None:
        self.logger = logger
        self.using = using
        self.db_params = db_params
        self.psycopg_options = PsycopgOptions(**psycopg_options)

    async def get_db_pool(
        self, db_params: dict[str, typing.Any]
    ) -> psycopg_pool.AsyncConnectionPool:
        global connection_pool

        async def _configure_connection(conn: psycopg.AsyncConnection) -> None:
            await conn.set_autocommit(True)

        async with is_creating_connection_pool:
            if connection_pool is not None:
                self.logger.debug('Pool %s already exists', connection_pool.name)

                pool_stats = connection_pool.get_stats()
                self.logger.debug('Pool stats: %s', pool_stats)

                return connection_pool

            conn_info = psycopg.conninfo.make_conninfo(conninfo='', **db_params)
            pool_connection_class = self.psycopg_options.connection_class
            connection_pool = pool_connection_class(
                conninfo=conn_info,
                open=False,
                configure=_configure_connection,
                min_size=2,
            )
            await connection_pool.open(wait=True)

            self.logger.debug('Pool %s created', connection_pool.name)

            return connection_pool

    async def _retrieve_group_channels(self, group_key: str) -> list[str]:
        query = GroupChannel.objects.filter(group_key=group_key).distinct('group_key', 'channel')
        channels = []
        async for channel in query:
            channels.append(channel.channel)

        return channels

    async def send_to_channel(
        self,
        group_key: str,
        message: bytes,
        expire: int,
        channel: typing.Optional[str] = None,
    ) -> None:
        """Send a message to a channel/channels (if no channel is specified)."""
        if channel is None:
            channels = await self._retrieve_group_channels(group_key)
            if not channels:
                self.logger.warning('Group: %s does not exist, did you call group_add?', group_key)
                return
        else:
            channels = [channel]

        # Bulk insert messages
        db_expiry = timezone.now() + timedelta(seconds=expire)
        messages = [
            Message(channel=channel, message=message, expire=db_expiry) for channel in channels
        ]

        await Message.objects.abulk_create(messages)

    async def add_channel_to_group(self, group_key: str, channel: str, expire: int) -> None:
        """Adds a channel to a group"""
        db_expiry = timezone.now() + timedelta(seconds=expire)
        await GroupChannel.objects.acreate(group_key=group_key, channel=channel, expire=db_expiry)

        self.logger.debug('Channel %s added to Group %s', channel, group_key)

    async def delete_expired_groups(self) -> None:
        """Deletes expired groups after a random delay"""
        expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired groups in %s seconds...', expire)

        await asyncio.sleep(expire)

        await GroupChannel.objects.filter(expire__lt=timezone.now()).adelete()

    async def delete_expired_messages(self, expire: typing.Optional[int] = None) -> None:
        """Deletes expired messages after a set time or random delay"""
        if expire is None:
            expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired messages in %s seconds...', expire)

        await asyncio.sleep(expire)

        await Message.objects.filter(expire__lt=timezone.now()).adelete()

    async def retrieve_non_expired_queued_message_from_channel(
        self, channel: str
    ) -> typing.Optional[tuple[bytes]]:
        """Retrieves a non-expired message from a channel"""
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
        db_pool = await self.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(retrieve_queued_messages_sql, (channel,))
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
