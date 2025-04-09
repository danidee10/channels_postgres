"""channels_postgres core"""

import asyncio
import base64
import hashlib
import logging
import platform
import sys
import typing
import uuid

import msgpack
from asgiref.sync import sync_to_async
from channels.layers import BaseChannelLayer
from django.db.backends.postgresql.base import DatabaseWrapper

from .db import DatabaseLayer

try:
    from cryptography.fernet import Fernet, MultiFernet

    CRYPTOGRAPHY_INSTALLED = True
except ImportError:
    CRYPTOGRAPHY_INSTALLED = False

# ProactorEventLoop is not supported by psycopg3 on windows
# https://www.psycopg.org/psycopg3/docs/advanced/async.html
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore

from asyncio import create_task  # pylint: disable=C0411,C0413

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    pass


async def asyncnext(ait: typing.AsyncIterator[typing.Any]) -> typing.Any:
    """
    asyncnext is a helper function that returns the next item from an async iterator.
    It is a backport of the built-in anext function that was added in Python 3.10.
    """
    if sys.version_info >= (3, 10):
        return await anext(ait)  # noqa: F821

    return await ait.__anext__()  # pylint: disable=C2801


class PostgresChannelLayer(BaseChannelLayer):  # type: ignore
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

    def __init__(  # pylint: disable=R0913,R0917
        self,
        prefix: str = 'asgi',
        expiry: int = 60,
        group_expiry: int = 0,
        symmetric_encryption_keys: typing.Any = None,
        **kwargs: dict[str, typing.Any],
    ):
        super().__init__(expiry=expiry)

        self.prefix = prefix
        self.group_expiry = group_expiry
        self.client_prefix = uuid.uuid4().hex[:5]
        self.crypter: typing.Optional[MultiFernet] = None
        self._setup_encryption(symmetric_encryption_keys)

        try:
            kwargs['OPTIONS']
        except KeyError:
            kwargs['OPTIONS'] = {}

        db_wrapper = DatabaseWrapper(kwargs)
        self.db_params = db_wrapper.get_connection_params()

        # Prevent psycopg from using the custom synchronous cursor factory from django
        self.db_params.pop('cursor_factory')
        self.db_params.pop('context')

        psycopg_options = kwargs.get('PYSCOPG_OPTIONS', {})
        self.django_db = DatabaseLayer(psycopg_options, self.db_params, logger=logger)

    def _setup_encryption(
        self, symmetric_encryption_keys: typing.Union[list[bytes], list[str]]
    ) -> None:
        # See if we can do encryption if they asked
        if symmetric_encryption_keys:
            if isinstance(symmetric_encryption_keys, (str, bytes)):
                raise ValueError('symmetric_encryption_keys must be a list of possible keys')
            if not CRYPTOGRAPHY_INSTALLED:
                raise ValueError('Cannot run with encryption without `cryptography` installed.')

            sub_fernets = [self.make_fernet(key) for key in symmetric_encryption_keys]
            self.crypter = MultiFernet(sub_fernets)

    def make_fernet(self, key: typing.Union[bytes, str]) -> 'Fernet':
        """
        Given a single encryption key, returns a Fernet instance using it.
        """
        if Fernet is None:
            raise ValueError('Cannot run with encryption without `cryptography` installed.')

        if isinstance(key, str):
            key = key.encode('utf-8')
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())

        return Fernet(formatted_key)

    # ==============================================================
    # Channel layer API
    # ==============================================================

    extensions = ['groups', 'flush']

    async def send(self, channel: str, message: dict[str, typing.Any]) -> None:
        """Send a message onto a (general or specific) channel."""
        # Typecheck
        assert isinstance(message, dict), 'message is not a dict'
        try:
            assert self.require_valid_channel_name(channel), 'Channel name not valid'
        except AttributeError:
            assert self.valid_channel_name(channel), 'Channel name not valid'
        # Make sure the message does not contain reserved keys
        assert '__asgi_channel__' not in message
        serialized_message = self.serialize(message)

        await self.django_db.send_to_channel('', serialized_message, self.expiry, channel=channel)

    # async def _get_message_from_channel(self, channel):
    #    retrieve_events_sql = f'LISTEN "{channel}";'

    #    while True:
    #        conn_info = psycopg.conninfo.make_conninfo(conninfo='', **self.db_params)
    #        async with await psycopg.AsyncConnection.connect(
    #            conninfo=conn_info, autocommit=True
    #        ) as conn:
    #            message = await self.django_db.retrieve_queued_message_from_channel(conn, channel)
    #            if not message:
    #                await conn.execute(retrieve_events_sql)
    #                try:
    #                    event = await asyncnext(conn.notifies(timeout=5.0))
    #                    message_id = event.payload
    #                    message = await self.django_db.delete_message_returning_message(
    #                        conn, message_id
    #                    )
    #                except StopAsyncIteration:
    #                    logger.debug(
    #                        'Expected timeout waiting for message on channel %s.
    #                        'Dropping db connection and reconnecting.',
    #                        channel,
    #                    )
    #                    continue

    #                message = self.deserialize(message[0])
    #                return message

    async def _get_message_from_channel(self, channel: str) -> dict[str, typing.Any]:
        retrieve_events_sql = f'LISTEN "{channel}";'

        db_pool = await self.django_db.get_db_pool(db_params=self.db_params)
        async with db_pool.connection() as conn:
            message = await self.django_db.retrieve_non_expired_queued_message_from_channel(channel)
            if not message:
                # Clear pending messages from the notifies generator from other connections
                await conn.execute('UNLISTEN *;')
                # without async_to_sync, Deque.clear() blocks the event loop
                await sync_to_async(conn._notifies_backlog.clear)()

                await conn.execute(retrieve_events_sql)

                event = await asyncnext(conn.notifies())
                message_id = event.payload
                message = await self.django_db.delete_message_returning_message(message_id)

            assert message is not None
            deserialized_message = self.deserialize(message[0])

            return deserialized_message

    async def receive(self, channel: str) -> dict[str, typing.Any]:
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

        return await self._get_message_from_channel(channel)

    async def new_channel(self, prefix: str = 'specific') -> str:
        """
        Returns a new channel name that can be used by something in our
        process as a specific channel.
        """
        return f'{prefix}.{self.client_prefix}!{uuid.uuid4().hex}'

    # ==============================================================
    # Flush extension
    # ==============================================================

    async def flush(self) -> None:
        """
        Unlisten on all channels and
        Deletes all messages and groups in the database
        """
        await self.django_db.flush()

    # ==============================================================
    # Groups extension
    # ==============================================================

    async def group_add(self, group: str, channel: str) -> None:
        """Adds the channel name to a group to the Postgres table."""
        group_key = self._group_key(group)

        await self.django_db.add_channel_to_group(group_key, channel, self.group_expiry)

    async def group_discard(
        self, group: str, channel: str, expire: typing.Optional[int] = None
    ) -> None:
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

    async def group_send(self, group: str, message: dict[str, typing.Any]) -> None:
        """Sends a message to the entire group."""
        try:
            assert self.require_valid_group_name(group), 'Group name not valid'
        except AttributeError:
            assert self.valid_group_name(group), 'Group name not valid'

        group_key = self._group_key(group)
        # Retrieve list of all channel names related to the group
        serialized_message = self.serialize(message)

        await self.django_db.send_to_channel(group_key, serialized_message, self.expiry)

    def _group_key(self, group: str) -> str:
        """Common function to make the storage key for the group."""
        return f'{self.prefix}:group:{group}'

    # ==============================================================
    # Serialization
    # ==============================================================

    def serialize(self, message: dict[str, typing.Any]) -> bytes:
        """Serializes message to a byte string."""
        value: bytes = msgpack.packb(message, use_bin_type=True)
        if self.crypter:
            value = self.crypter.encrypt(value)

        return value

    def deserialize(self, message: bytes) -> dict[str, typing.Any]:
        """Deserializes from a byte string."""
        if self.crypter:
            message = self.crypter.decrypt(message, self.expiry + 10)

        deserialized_message: dict[str, typing.Any] = msgpack.unpackb(message, raw=False)

        return deserialized_message
