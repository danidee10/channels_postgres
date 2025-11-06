"""common db methods."""

import asyncio
import hashlib
import logging
import random
import typing
from datetime import datetime, timedelta



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
    Encapsulates database operations using Django ORM for async operations.
    Replaces handcrafted psycopg3 queries for simplicity, maintainability,
    and native Django 5.1 connection pooling support.
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

    
    async def retrieve_group_channels(self, group_key: str) -> list[str]:
        """Retrieves all channels for a group"""
        qs = GroupChannel.objects.using(self.using).filter(group_key=group_key)
        return list(await qs.values_list('channel', flat=True))
    async def add_channel_to_group(self, group_key: str, channel: str, expire: int) -> None:
        """Adds a channel to a group"""
        expiry_datetime = utc_now() + timedelta(seconds=expire)
        await GroupChannel.objects.using(self.using).acreate(
            group_key=group_key, channel=channel, expire=expiry_datetime
        )

        self.logger.debug('Channel %s added to Group %s', channel, group_key)

    async def delete_expired_groups(self) -> None:
        """Deletes expired groups after a random delay"""
        delay = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired groups in %s seconds...', delay)
        await asyncio.sleep(delay)
        now = utc_now()
        await GroupChannel.objects.using(self.using).filter(expire__lt=now).adelete()

    async def delete_expired_messages(self, expire: typing.Optional[int] = None) -> None:
        """Deletes expired messages after a set time or random delay"""
        if expire is None:
            expire = 60 * random.randint(10, 20)
        self.logger.debug('Deleting expired messages in %s seconds...', expire)
        await asyncio.sleep(expire)
        now = utc_now()
        await Message.objects.using(self.using).filter(expire__lt=now).adelete()

    async def retrieve_non_expired_queued_messages(self) -> list[tuple[str, str, bytes, str]]:
        """
        Retrieves all non-expired messages from the database

        NOTE: Postgres doesn't support ORDER BY for `RETURNING`
        queries. Even if the inner query is ordered, the returning
        clause is not guaranteed to be ordered
        """

        now = utc_now()
        msgs = await Message.objects.using(self.using).filter(expire__gt=now).all()
        result = [(str(m.id), m.channel, m.message, str(m.expire.timestamp())) for m in msgs]
        await Message.objects.using(self.using).filter(id__in=[m.id for m in msgs]).adelete()
        return result
    async def retrieve_non_expired_queued_message_from_channel(
        self, channel: str
    ) -> typing.Optional[tuple[bytes]]:
        """Retrieves a non-expired message from a channel"""

        now = utc_now()
        msg = await Message.objects.using(self.using).filter(channel=channel, expire__gt=now).afirst()
        if msg:
            result = (msg.message,)
            await Message.objects.using(self.using).filter(id=msg.id).adelete()
            return result
        return None
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
        
        """Acquires an advisory lock (if still needed, else can remove)"""
        # keep for compatibility if using PostgreSQL advisory locks
        return True  # placeholder for ORM-only setup

    async def delete_message_returning_message(
        self, message_id: int
    ) -> typing.Optional[tuple[bytes]]:
        """Deletes a message from the database and returns the message"""
        msg = await Message.objects.using(self.using).filter(id=message_id).afirst()
        if msg:
            result = (msg.message,)
            await Message.objects.using(self.using).filter(id=msg.id).adelete()
            return result
        return None
     
    async def delete_channel_group(self, group_key: str, channel: str) -> None:
        """Deletes a channel from a group"""
        await (
            GroupChannel.objects.using(self.using)
            .filter(group_key=group_key, channel=channel)
            .adelete()
        )

    async def flush(self) -> None:
        """
        Flushes the channel layer by truncating the message and group tables
        """
        await Message.objects.using(self.using).adelete()
        await GroupChannel.objects.using(self.using).adelete()