"""channels_postgres core"""

import asyncio
import base64
import hashlib
import logging
import platform
import sys
import threading
import typing
import uuid
from concurrent.futures import ThreadPoolExecutor

import msgpack
import psycopg
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


ASYNCIO_EVENT_CHANNEL_MAPPING: dict[str, asyncio.Future[str]] = {}
AsyncGeneratorType = typing.TypeVar('AsyncGeneratorType')

listener_task_is_running = threading.Event()


async def asyncnext(
    async_generator: typing.AsyncGenerator[AsyncGeneratorType, None],
) -> AsyncGeneratorType:
    """
    asyncnext is a helper function that returns the next item from an async iterator.
    It is a backport of the built-in anext function that was added in Python 3.10.
    """
    if sys.version_info >= (3, 10):
        return await anext(async_generator)  # noqa: F821

    return await async_generator.__anext__()  # pylint: disable=C2801


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
        self.start_listener_task()

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

    def start_listener_task(self):
        global listener_task_is_running
        if listener_task_is_running.is_set():
            logger.debug('The listener task is already running')
            return

        executor = ThreadPoolExecutor(1)
        executor.submit(self.listen_to_all_channels)

    def listen_to_all_channels(self):
        retrieve_events_sql = 'LISTEN channels_postgres_message;'

        conn_info = psycopg.conninfo.make_conninfo(conninfo='', **self.db_params)
        with psycopg.Connection.connect(conninfo=conn_info, autocommit=True) as connection:
            connection.execute(retrieve_events_sql)

            # The db connection is open and now listening for events
            listener_task_is_running.set()

            for event in connection.notifies():
                if event.payload == '1:shutdown':
                    connection.notifies().close()
                    break

                split_payload = event.payload.split(':')
                channel = split_payload[1]
                future = ASYNCIO_EVENT_CHANNEL_MAPPING.get(channel, None)
                if future:
                    future.set_result(event.payload)

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
        while not listener_task_is_running.is_set():
            await asyncio.sleep(0.1)

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

    async def _get_message_from_channel(self, channel: str) -> dict[str, typing.Any]:
        message = await self.django_db.retrieve_non_expired_queued_message_from_channel(channel)
        if not message:
            # Create an asyncio future to wait for the message
            event_loop = asyncio.get_event_loop()
            future = event_loop.create_future()
            ASYNCIO_EVENT_CHANNEL_MAPPING[channel] = future

            # Receive the message and remove the future from the mapping
            event_payload = await future
            split_payload = event_payload.split(':')

            if len(split_payload) == 2:
                # The message is <= 7168 bytes, we don't need to fetch the message from the database
                message_id, base64_message = split_payload

                # There's is a problem with asyncio in python 3.9 where the base64.b64decode
                # somehow breaks the event loop and causes psycopg to have a different event loop
                # than the one that is running the receive coroutine.
                if sys.version_info >= (3, 10):
                    message = (base64.b64decode(base64_message),)
                else:
                    loop = asyncio.get_running_loop()
                    message_bytes = await loop.run_in_executor(
                        None, base64.b64decode, base64_message
                    )
                    message = (message_bytes,)
            else:
                message_id = split_payload[0]
                ASYNCIO_EVENT_CHANNEL_MAPPING.pop(channel)
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
        while not listener_task_is_running.is_set():
            await asyncio.sleep(0.1)

        try:
            assert self.require_valid_channel_name(channel), 'Channel name not valid'
        except AttributeError:
            assert self.valid_channel_name(channel), 'Channel name not valid'
        if '!' in channel:
            real_channel = self.non_local_name(channel)
            assert real_channel.endswith(self.client_prefix + '!'), 'Wrong client prefix'

        # Get the message from the channel
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
