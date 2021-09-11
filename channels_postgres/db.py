"""common db methods."""

import random
import asyncio
import logging

import aiopg


class DatabaseLayer:
    def __init__(self, using='channels_postgres', logger=None):
        self.logger = logger
        self.using = using

        if not self.logger:
            self.logger = logging.getLogger('channels_postgres.database')

    async def _retrieve_group_channels(self, cur, group_key):
        retrieve_channels_sql = (
            'SELECT DISTINCT group_key,channel '
            'FROM channels_postgres_groupchannel WHERE group_key=%s;'
        )

        await cur.execute(retrieve_channels_sql, (group_key,))

        channels = []
        async for row in cur:
            channels.append(row[1])

        return channels

    async def send_to_channel(self, conn, group_key, message, expire, channel=None):
        """Send a message to a channel/channels (if no channel is specified)."""
        cur = await conn.cursor()
        if channel is None:
            channels = await self._retrieve_group_channels(cur, group_key)
            if not channels:
                self.logger.warning(
                    f"Group: {group_key} does not exist, did you call group_add?"
                )
                return
        else:
            channels = [channel]

        values_str = b','.join(
            cur.mogrify(
                "(%s, %s, (NOW() + INTERVAL '%s seconds'))", (channel, message, expire)
            ) for channel in channels
        )
        insert_message_sql = (
            b'INSERT INTO channels_postgres_message (channel, message, expire) VALUES ' + values_str
        )

        await cur.execute(insert_message_sql)

    async def add_channel_to_group(self, conn, group_key, channel, expire):
        group_add_sql = (
            'INSERT INTO channels_postgres_groupchannel (group_key, channel, expire) '
            "VALUES (%s, %s, (NOW() + INTERVAL '%s seconds'))"
        )

        cur = await conn.cursor()
        await cur.execute(group_add_sql, (group_key, channel, expire))

        self.logger.debug('Channel %s added to Group %s', channel, group_key)

    async def delete_expired_groups(self, db_params):
        expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired groups in %s seconds...', expire)

        await asyncio.sleep(60)
        delete_sql = (
            'DELETE FROM channels_postgres_groupchannel '
            'WHERE expire < NOW()'
        )
        async with aiopg.connect(**db_params) as conn:
            cur = await conn.cursor()
            await cur.execute(delete_sql)

    async def delete_expired_messages(self, db_params, expire):
        if expire is None:
            expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired messages in %s seconds...', expire)

        await asyncio.sleep(expire)
        delete_sql = (
            'DELETE FROM channels_postgres_message '
            'WHERE expire < NOW()'
        )
        async with aiopg.connect(**db_params) as conn:
            cur = await conn.cursor()
            await cur.execute(delete_sql)
