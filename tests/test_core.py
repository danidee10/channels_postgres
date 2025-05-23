"""channels_postgres tests"""

import asyncio
import random
import typing

import async_timeout
import django
import psycopg
import pytest
from asgiref.sync import async_to_sync
from django.conf import settings

django.setup()

from asyncio import create_task  # noqa: E402  pylint: disable=C0411,C0413

from django.db.backends.postgresql.base import (  # noqa: E402  pylint: disable=C0411,C0413
    DatabaseWrapper,
)

from channels_postgres.core import PostgresChannelLayer  # noqa: E402  pylint: disable=C0411,C0413

default_layer_config: dict[str, typing.Any] = {
    'prefix': 'asgi',
    'expiry': 60,
    'group_expiry': 0,
    'symmetric_encryption_keys': None,
    'config': None,
}


@pytest.fixture(scope='function', autouse=True)
async def shutdown_listener() -> typing.AsyncGenerator[None, None]:
    """
    Fixture that shuts down the listener
    """
    yield

    db_wrapper = DatabaseWrapper(settings.DATABASES['channels_postgres'])
    db_params = db_wrapper.get_connection_params()
    db_params.pop('cursor_factory')
    db_params.pop('context')
    conn_info = psycopg.conninfo.make_conninfo(conninfo='', **db_params)
    async with await psycopg.AsyncConnection.connect(conninfo=conn_info, autocommit=True) as conn:
        await conn.execute("NOTIFY channels_postgres_message, '1:shutdown';")


@pytest.fixture(name='channel_layer', autouse=True)
async def channel_layer_fixture() -> typing.AsyncGenerator[PostgresChannelLayer, None]:
    """Channel layer fixture that flushes automatically."""
    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None

    channel_layer = PostgresChannelLayer(**default_layer_config, **db_params)

    yield channel_layer

    await channel_layer.flush()


async def send_three_messages_with_delay(
    channel_name: str, channel_layer: PostgresChannelLayer, delay: int
) -> None:
    """
    Sends three messages to a channel with a delay between each message.

    The messages should be sent without errors.
    """
    await channel_layer.send(channel_name, {'type': 'test.message', 'text': 'First!'})

    await asyncio.sleep(delay)

    await channel_layer.send(channel_name, {'type': 'test.message', 'text': 'Second!'})

    await asyncio.sleep(delay)

    await channel_layer.send(channel_name, {'type': 'test.message', 'text': 'Third!'})


async def group_send_three_messages_with_delay(
    group_name: str, channel_layer: PostgresChannelLayer, delay: int
) -> None:
    """
    Sends three messages to a group with a delay between each message.

    The messages should be sent without errors.
    """
    await channel_layer.group_send(group_name, {'type': 'test.message', 'text': 'First!'})

    await asyncio.sleep(delay)

    await channel_layer.group_send(group_name, {'type': 'test.message', 'text': 'Second!'})

    await asyncio.sleep(delay)

    await channel_layer.group_send(group_name, {'type': 'test.message', 'text': 'Third!'})


@pytest.mark.asyncio
async def test_send_receive_basic(channel_layer: PostgresChannelLayer) -> None:
    """Makes sure we can send a message to a normal channel and receive it."""
    await channel_layer.send('test-channel-1', {'type': 'test.message', 'text': 'Ahoy-hoy!'})
    message = await channel_layer.receive('test-channel-1')

    assert message['type'] == 'test.message'
    assert message['text'] == 'Ahoy-hoy!'


@pytest.mark.asyncio
async def test_send_received(channel_layer: PostgresChannelLayer) -> None:
    """
    Similar to the `test_send_receive_basic`
    but mimics a real world scenario where clients connect first
    and wait for messages
    """

    task = create_task(channel_layer.receive('test-channel-2'))

    async def chained_tasks() -> None:
        await asyncio.sleep(1)
        await channel_layer.send(
            'test-channel-2', {'type': 'test.message_connect_wait', 'text': 'Hello world!'}
        )

    await asyncio.wait([task, create_task(chained_tasks())])

    message = task.result()
    assert message['type'] == 'test.message_connect_wait'
    assert message['text'] == 'Hello world!'


@pytest.mark.parametrize('channel_layer', [None])  # Fixture can't handle sync
def test_double_receive(channel_layer: PostgresChannelLayer) -> None:
    """
    Makes sure we can receive from two different event loops using
    process-local channel names.
    """
    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None
    channel_layer = PostgresChannelLayer(**default_layer_config, **db_params)

    channel_name_1 = async_to_sync(channel_layer.new_channel)()
    channel_name_2 = async_to_sync(channel_layer.new_channel)()
    async_to_sync(channel_layer.send)(channel_name_1, {'type': 'test.message.1'})
    async_to_sync(channel_layer.send)(channel_name_2, {'type': 'test.message.2'})

    # Define listeners
    async def listen1() -> None:
        message = await channel_layer.receive(channel_name_1)
        assert message['type'] == 'test.message.1'

    async def listen2() -> None:
        message = await channel_layer.receive(channel_name_2)
        assert message['type'] == 'test.message.2'

    # Run them inside threads to ensure that they are running in different event loops
    async_to_sync(listen2)()
    async_to_sync(listen1)()

    # Clean up
    async_to_sync(channel_layer.flush)()


@pytest.mark.asyncio
async def test_process_local_send_receive(channel_layer: PostgresChannelLayer) -> None:
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    channel_name = await channel_layer.new_channel()
    await channel_layer.send(channel_name, {'type': 'test.message', 'text': 'Local only please'})
    message = await channel_layer.receive(channel_name)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Local only please'


@pytest.mark.asyncio
async def test_multi_send_receive(channel_layer: PostgresChannelLayer) -> None:
    """Tests overlapping sends and receives, and ordering."""
    await channel_layer.send('test-channel-3', {'type': 'message.1'})
    await channel_layer.send('test-channel-3', {'type': 'message.2'})
    await channel_layer.send('test-channel-3', {'type': 'message.3'})
    assert (await channel_layer.receive('test-channel-3'))['type'] == 'message.1'
    assert (await channel_layer.receive('test-channel-3'))['type'] == 'message.2'
    assert (await channel_layer.receive('test-channel-3'))['type'] == 'message.3'


@pytest.mark.asyncio
async def test_reject_bad_channel(channel_layer: PostgresChannelLayer) -> None:
    """
    Makes sure sending/receiving on an invalic channel name fails.
    """
    with pytest.raises(TypeError):
        await channel_layer.send('=+135!', {'type': 'foom'})
    with pytest.raises(TypeError):
        await channel_layer.receive('=+135!')


@pytest.mark.asyncio
async def test_reject_bad_client_prefix(channel_layer: PostgresChannelLayer) -> None:
    """
    Makes sure receiving on a non-prefixed local channel is not allowed.
    """
    with pytest.raises(AssertionError):
        await channel_layer.receive('not-client-prefix!local_part')


@pytest.mark.asyncio
async def test_non_existent_group(channel_layer: PostgresChannelLayer) -> None:
    """sending on Non-existent groups shoudldn't raise any Exceptions or send any message."""
    try:
        await channel_layer.group_send('non-existent', {'type': 'message.1'})
    except Exception as e:  # pylint: disable=W0718
        pytest.fail(f'Unexpected exception {e}')


@pytest.mark.asyncio
async def test_groups_basic(channel_layer: PostgresChannelLayer) -> None:
    """
    Tests basic group operation.
    """
    channel_name1 = await channel_layer.new_channel(prefix='test-gr-chan-1')
    channel_name2 = await channel_layer.new_channel(prefix='test-gr-chan-2')
    channel_name3 = await channel_layer.new_channel(prefix='test-gr-chan-3')
    await channel_layer.group_add('test-group', channel_name1)
    await channel_layer.group_add('test-group', channel_name2)
    await channel_layer.group_add('test-group', channel_name3)
    await channel_layer.group_discard('test-group', channel_name2, expire=1)
    await channel_layer.group_send('test-group', {'type': 'message.1'})
    # Make sure we get the message on the two channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))['type'] == 'message.1'
        assert (await channel_layer.receive(channel_name3))['type'] == 'message.1'
    # Make sure the removed channel did not get the message
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name2)


@pytest.mark.asyncio
async def test_groups_same_prefix(channel_layer: PostgresChannelLayer) -> None:
    """
    Tests group_send with multiple channels with same channel prefix
    """
    channel_name1 = await channel_layer.new_channel(prefix='test-gr-chan')
    channel_name2 = await channel_layer.new_channel(prefix='test-gr-chan')
    channel_name3 = await channel_layer.new_channel(prefix='test-gr-chan')
    await channel_layer.group_add('test-group', channel_name1)
    await channel_layer.group_add('test-group', channel_name2)
    await channel_layer.group_add('test-group', channel_name3)
    await channel_layer.group_send('test-group', {'type': 'message.1'})

    # Make sure we get the message on the channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))['type'] == 'message.1'
        assert (await channel_layer.receive(channel_name2))['type'] == 'message.1'
        assert (await channel_layer.receive(channel_name3))['type'] == 'message.1'


@pytest.mark.asyncio
async def test_receive_cancel(channel_layer: PostgresChannelLayer) -> None:
    """
    Makes sure we can cancel a receive without blocking
    """
    channel = await channel_layer.new_channel()
    delay = 0.0
    while delay < 0.01:
        await channel_layer.send(channel, {'type': 'test.message', 'text': 'Ahoy-hoy!'})

        task = asyncio.ensure_future(channel_layer.receive(channel))
        await asyncio.sleep(delay)
        task.cancel()
        delay += 0.0001

        try:
            await asyncio.wait_for(task, None)
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_random_reset__channel_name(channel_layer: PostgresChannelLayer) -> None:
    """
    Makes sure resetting random seed does not make us reuse channel names.
    """
    random.seed(1)
    channel_name_1 = await channel_layer.new_channel()
    random.seed(1)
    channel_name_2 = await channel_layer.new_channel()

    assert channel_name_1 != channel_name_2


@pytest.mark.asyncio
async def test_random_reset__client_prefix() -> None:
    """
    Makes sure resetting random seed does not make us reuse client_prefixes.
    """
    random.seed(1)
    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None

    channel_layer_1 = PostgresChannelLayer(**default_layer_config, **db_params)
    random.seed(1)
    channel_layer_2 = PostgresChannelLayer(**default_layer_config, **db_params)
    assert channel_layer_1.client_prefix != channel_layer_2.client_prefix


@pytest.mark.asyncio
async def test_message_expiry__earliest_message_expires(
    channel_layer: PostgresChannelLayer,
) -> None:
    """
    Tests message expiry

    The channel layer should not return expired messages.
    """
    expiry = 3
    delay = 2
    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None

    default_layer_config['expiry'] = expiry
    channel_layer = PostgresChannelLayer(**default_layer_config, **db_params)
    channel_name = await channel_layer.new_channel()

    task = asyncio.ensure_future(send_three_messages_with_delay(channel_name, channel_layer, delay))
    await asyncio.wait_for(task, None)

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_name)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Second!'

    message = await channel_layer.receive(channel_name)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Third!'

    # Make sure there's no third message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name)


@pytest.mark.asyncio
async def test_message_expiry__all_messages_under_expiration_time() -> None:
    """
    Tests message expiry

    The channel layer should return all messages if they are not expired.
    """
    expiry = 3
    delay = 1
    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None

    default_layer_config['expiry'] = expiry
    channel_layer = PostgresChannelLayer(**default_layer_config, **db_params)
    channel_name = await channel_layer.new_channel()

    task = asyncio.ensure_future(send_three_messages_with_delay(channel_name, channel_layer, delay))
    await asyncio.wait_for(task, None)

    # expiry = 3, total delay under 3, all messages there
    message = await channel_layer.receive(channel_name)
    assert message['type'] == 'test.message'
    assert message['text'] == 'First!'

    message = await channel_layer.receive(channel_name)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Second!'

    message = await channel_layer.receive(channel_name)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Third!'


@pytest.mark.asyncio
async def test_message_expiry__group_send() -> None:
    """
    Tests group messages expiry

    The channel layer should not return expired group messages.
    """
    expiry = 3
    delay = 2
    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None

    default_layer_config['expiry'] = expiry
    channel_layer = PostgresChannelLayer(**default_layer_config, **db_params)
    channel_name = await channel_layer.new_channel()

    await channel_layer.group_add('test-group', channel_name)

    task = asyncio.ensure_future(
        group_send_three_messages_with_delay('test-group', channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_name)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Second!'

    message = await channel_layer.receive(channel_name)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Third!'

    # Make sure there's no third message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name)


@pytest.mark.asyncio
async def test_message_expiry__group_send__one_channel_expires_message() -> None:
    """
    Tests group messages expiry

    The channel layer should not return expired group messages.
    """
    expiry = 4
    delay = 1

    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None

    default_layer_config['expiry'] = expiry
    channel_layer = PostgresChannelLayer(**default_layer_config, **db_params)
    channel_1 = await channel_layer.new_channel()
    channel_2 = await channel_layer.new_channel(prefix='channel_2')

    await channel_layer.group_add('test-group', channel_1)
    await channel_layer.group_add('test-group', channel_2)

    # Let's give channel_1 one additional message and then sleep
    await channel_layer.send(channel_1, {'type': 'test.message', 'text': 'Zero!'})
    await asyncio.sleep(2)

    task = asyncio.ensure_future(
        group_send_three_messages_with_delay('test-group', channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # message Zero! was sent about 2 + 1 + 1 seconds ago and it should have expired
    message = await channel_layer.receive(channel_1)
    assert message['type'] == 'test.message'
    assert message['text'] == 'First!'

    message = await channel_layer.receive(channel_1)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Second!'

    message = await channel_layer.receive(channel_1)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Third!'

    # Make sure there's no fourth message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_1)

    # channel_2 should receive all three messages from group_send
    message = await channel_layer.receive(channel_2)
    assert message['type'] == 'test.message'
    assert message['text'] == 'First!'

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_2)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Second!'

    message = await channel_layer.receive(channel_2)
    assert message['type'] == 'test.message'
    assert message['text'] == 'Third!'


@pytest.mark.asyncio
async def test_guarantee_at_most_once_delivery() -> None:
    """
    Tests that at most once delivery is guaranteed.

    If two consumers are listening on the same channel,
    the message should be delivered to only one of them.
    """
    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None

    channel_name = 'same-channel'
    loop = asyncio.get_running_loop()

    channel_layer = PostgresChannelLayer(**default_layer_config, **db_params)
    channel_layer_2 = PostgresChannelLayer(**default_layer_config, **db_params)
    future_channel_layer = loop.create_future()
    future_channel_layer_2 = loop.create_future()

    async def receive_task(
        channel_layer: PostgresChannelLayer, future: asyncio.Future[typing.Any]
    ) -> None:
        message = await channel_layer.receive(channel_name)
        future.set_result(message)

    # Ensure that receive_task_2 is scheduled first and accquires the advisory lock
    create_task(receive_task(channel_layer_2, future_channel_layer_2))
    while channel_layer_2.listener_task_is_running is None:
        await asyncio.sleep(0.1)
    await channel_layer_2.listener_task_is_running.wait()

    create_task(receive_task(channel_layer, future_channel_layer))
    while channel_layer.listener_task_is_running is None:
        await asyncio.sleep(0.1)
    await channel_layer.listener_task_is_running.wait()

    await channel_layer.send(channel_name, {'type': 'test.message', 'text': 'Hello!'})

    result = await future_channel_layer_2
    assert result['type'] == 'test.message'
    assert result['text'] == 'Hello!'

    # Channel layer 1 should not receive the message
    # as it is already consumed by channel layer 2
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await future_channel_layer


def test_default_group_key_format() -> None:
    """
    Tests the default group key format.
    """
    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None

    channel_layer = PostgresChannelLayer(**default_layer_config, **db_params)
    group_name = channel_layer._group_key('test_group')  # pylint: disable=W0212
    assert group_name == 'asgi:group:test_group'


def test_custom_group_key_format() -> None:
    """
    Tests the custom group key format.
    """
    db_params: dict[str, typing.Any] | None = settings.DATABASES.get('channels_postgres', None)
    assert db_params is not None

    default_layer_config['prefix'] = 'test_prefix'
    channel_layer = PostgresChannelLayer(**default_layer_config, **db_params)
    group_name = channel_layer._group_key('test_group')  # pylint: disable=W0212
    assert group_name == 'test_prefix:group:test_group'
