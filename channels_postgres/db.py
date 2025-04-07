"""common db methods."""

import random
import asyncio
import logging
import psycopg

from django.utils import timezone
from django.utils.timezone import timedelta

from .models import GroupChannel, Message


class DatabaseLayer:
    def __init__(self, db_params, using='channels_postgres', logger=None):
        self.logger = logger
        self.using = using
        self.db_params = db_params

        if not self.logger:
            self.logger = logging.getLogger('channels_postgres.database')

    async def _retrieve_group_channels(self, group_key):
        query = GroupChannel.objects.filter(group_key=group_key).distinct('group_key', 'channel')
        channels = []
        async for channel in query:
            channels.append(channel.channel)

        return channels

    async def send_to_channel(self, group_key, message, expire, channel=None):
        """Send a message to a channel/channels (if no channel is specified)."""
        if channel is None:
            channels = await self._retrieve_group_channels(group_key)
            if not channels:
                self.logger.warning(f'Group: {group_key} does not exist, did you call group_add?')
                return
        else:
            channels = [channel]

        # Bulk insert messages
        db_expiry = timezone.now() + timedelta(seconds=expire)
        messages = [
            Message(channel=channel, message=message, expire=db_expiry) for channel in channels
        ]

        await Message.objects.abulk_create(messages)

    async def add_channel_to_group(self, group_key, channel, expire):
        db_expiry = timezone.now() + timedelta(seconds=expire)
        await GroupChannel.objects.acreate(group_key=group_key, channel=channel, expire=db_expiry)

        self.logger.debug('Channel %s added to Group %s', channel, group_key)

    async def delete_expired_groups(self):
        expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired groups in %s seconds...', expire)

        await asyncio.sleep(expire)

        await GroupChannel.objects.filter(expire__lt=timezone.now()).adelete()

    async def delete_expired_messages(self, expire):
        if expire is None:
            expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired messages in %s seconds...', expire)

        await asyncio.sleep(expire)

        await Message.objects.filter(expire__lt=timezone.now()).adelete()

    async def retrieve_queued_message_from_channel(self, conn, channel):
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
            RETURNING message, expire;
        """
        async with conn.cursor() as cursor:
            await cursor.execute(retrieve_queued_messages_sql, (channel,))

            return await cursor.fetchone()

    async def delete_message_returning_message(self, conn, message_id):
        delete_message_returning_message_sql = (
            'DELETE FROM channels_postgres_message WHERE id=%s RETURNING message;'
        )

        async with conn.cursor() as cursor:
            await cursor.execute(delete_message_returning_message_sql, (message_id,))

            return await cursor.fetchone()

    async def delete_channel_group(self, group_key, channel):
        await GroupChannel.objects.filter(group_key=group_key, channel=channel).adelete()

    async def flush(self):
        async with await psycopg.AsyncConnection.connect(**self.db_params, autocommit=True) as conn:
            await conn.execute('UNLISTEN *;')
            await conn.execute('TRUNCATE TABLE "{0}"'.format(Message._meta.db_table))
            await conn.execute('TRUNCATE TABLE "{0}"'.format(GroupChannel._meta.db_table))
