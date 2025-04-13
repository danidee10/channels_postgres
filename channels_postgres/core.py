"""channels_postgres core"""

import asyncio
import base64
import hashlib
import logging
import platform
import time
import typing
import uuid

import msgpack
import psycopg
from channels.layers import BaseChannelLayer
from django.db.backends.postgresql.base import DatabaseWrapper

from .db import DatabaseLayer

try:
    from cryptography.fernet import Fernet, MultiFernet
except ImportError:
    MultiFernet = Fernet = None  # type: ignore

# ProactorEventLoop is not supported by psycopg3 on windows
# https://www.psycopg.org/psycopg3/docs/advanced/async.html
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore

from asyncio import create_task  # pylint: disable=C0411,C0413

logger = logging.getLogger(__name__)


event_mapping_lock = asyncio.Lock()
ASYNCIO_EVENT_CHANNEL_MAPPING: dict[str, asyncio.Queue[str]] = {}


class PostgresChannelLayer(BaseChannelLayer):  # type: ignore  # pylint: disable=R0902
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

        self.event_loop: typing.Optional[asyncio.AbstractEventLoop] = None
        self.listener_task_is_running: typing.Optional[asyncio.Event] = None

        self.prefix = prefix
        self.group_expiry = group_expiry
        self.client_prefix = uuid.uuid4().hex[:5]
        self.crypter: typing.Optional['MultiFernet'] = None
        self._setup_encryption(symmetric_encryption_keys)

        try:
            kwargs['OPTIONS']
        except KeyError:
            kwargs['OPTIONS'] = {}

        db_wrapper = DatabaseWrapper(kwargs)

        # Prevent psycopg from using the custom synchronous cursor factory from django
        db_params = db_wrapper.get_connection_params()
        db_params.pop('cursor_factory')
        db_params.pop('context')
        self.db_params = db_params

        psycopg_options = kwargs.get('PYSCOPG_OPTIONS', {})
        self.django_db = DatabaseLayer(psycopg_options, self.db_params, logger=logger)

    def _setup_encryption(
        self, symmetric_encryption_keys: typing.Union[list[bytes], list[str]]
    ) -> None:
        # See if we can do encryption if they asked
        if symmetric_encryption_keys:
            if isinstance(symmetric_encryption_keys, (str, bytes)):
                raise ValueError('symmetric_encryption_keys must be a list of possible keys')
            if MultiFernet is None:
                raise ValueError('Cannot run with encryption without `cryptography` installed.')

            sub_fernets = [self.make_fernet(key) for key in symmetric_encryption_keys]
            self.crypter = MultiFernet(sub_fernets)

    def _get_or_create_listener_task(self) -> tuple[asyncio.Event, bool]:
        if not self.listener_task_is_running:
            self.listener_task_is_running = asyncio.Event()

        if not self.event_loop:
            self.event_loop = asyncio.get_running_loop()

        # self.event_loop and running_event_loop can be different if a different thread is started
        running_event_loop = asyncio.get_running_loop()
        if self.listener_task_is_running.is_set() and self.event_loop == running_event_loop:
            return self.listener_task_is_running, False

        return self.listener_task_is_running, True

    async def listen_to_all_channels(self) -> None:
        """
        Listens for messages in all channels from the database
        and sends them to the respective queue(s)
        """

        # Retrieve all non-expired messages for all channels from the database
        # and send them to the respective queues
        # The results need to be ordered by id
        # as the database returns them in an arbitrary order
        returning = await self.django_db.retrieve_non_expired_queued_messages()
        returning.sort(key=lambda x: int(x[0]))

        for returning_message in returning:
            message_id, channel, message, timestamp = returning_message
            base64_message = base64.b64encode(message).decode('utf-8')
            event_payload = f'{message_id}:{channel}:{base64_message}:{timestamp}'
            queue = await self._get_or_create_queue(channel)
            await queue.put(event_payload)

        retrieve_events_sql = 'LISTEN channels_postgres_message;'

        conn_info = psycopg.conninfo.make_conninfo(conninfo='', **self.db_params)
        async with await psycopg.AsyncConnection.connect(
            conninfo=conn_info, autocommit=True
        ) as connection:
            await connection.execute(retrieve_events_sql)

            # The db connection is open and now listening for events
            assert self.listener_task_is_running is not None
            self.listener_task_is_running.set()

            # This is a blocking call that will wait for events
            # until the generator is closed
            async for event in connection.notifies():
                if event.payload == '1:shutdown':
                    logger.debug('Shutting down listener task')
                    self.listener_task_is_running.clear()
                    await connection.notifies().aclose()
                    break

                split_payload = event.payload.split(':')
                message_id, channel = (split_payload[0], split_payload[1])

                # delete the message from the database
                await self.django_db.delete_message_returning_message(int(message_id))

                queue = await self._get_or_create_queue(channel)
                await queue.put(event.payload)

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

    async def _get_or_create_queue(self, channel: str) -> asyncio.Queue[str]:
        async with event_mapping_lock:
            queue = ASYNCIO_EVENT_CHANNEL_MAPPING.get(channel, None)
            if not queue:
                queue = asyncio.Queue()
                ASYNCIO_EVENT_CHANNEL_MAPPING[channel] = queue

        return queue

    async def send(self, channel: str, message: dict[str, typing.Any]) -> None:
        """Send a message onto a (general or specific) channel."""
        await self._get_or_create_queue(channel)

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

    async def _get_message_from_channel(
        self, channel: str, queue: asyncio.Queue[str]
    ) -> dict[str, typing.Any]:
        logger.debug('Getting message from channel %s', channel)
        message: typing.Optional[tuple[bytes]] = None
        while True:
            # Receive the message and remove the future from the mapping
            event_payload = await queue.get()
            split_payload = event_payload.split(':')

            if len(split_payload) == 4:
                # The message is <= 7168 bytes, we don't need to fetch the message from the database
                # The message doesn't need to be deleted from the database immediately. It will be
                # cleaned up later by the delete_expired_messages function.
                # also we never retrieve expired messages from the database
                message_id, _, base64_message, timestamp = split_payload
                message = (base64.b64decode(base64_message),)
                if float(timestamp) < time.time():
                    continue
                message = (base64.b64decode(base64_message),)
            else:
                message_id = split_payload[0]
                message = await self.django_db.delete_message_returning_message(int(message_id))
                assert message is not None

            break

        deserialized_message = self.deserialize(message[0])

        return deserialized_message

    async def receive(self, channel: str) -> dict[str, typing.Any]:
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine waits on the same channel, the first waiter
        will be given the message when it arrives.
        """
        queue = await self._get_or_create_queue(channel)
        listener_task, is_new_task = self._get_or_create_listener_task()
        if is_new_task:
            asyncio.create_task(self.listen_to_all_channels())
        await listener_task.wait()

        try:
            assert self.require_valid_channel_name(channel), 'Channel name not valid'
        except AttributeError:
            assert self.valid_channel_name(channel), 'Channel name not valid'
        if '!' in channel:
            real_channel = self.non_local_name(channel)
            assert real_channel.endswith(self.client_prefix + '!'), 'Wrong client prefix'

        # Get the message from the channel
        return await self._get_message_from_channel(channel, queue)

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
        channels = await self.django_db.retrieve_group_channels(group)
        for channel in channels:
            await self._get_or_create_queue(channel)

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
