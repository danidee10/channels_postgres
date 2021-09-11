import sys
import random
import asyncio

import django
from django.conf import settings

import pytest
import async_timeout
from asgiref.sync import async_to_sync

django.setup()  # noqa
from channels_postgres.core import PostgresChannelLayer  # noqa


if sys.version_info < (3, 7):
    create_task = asyncio.ensure_future
else:
    from asyncio import create_task


@pytest.fixture(scope='module')
def event_loop():
    """Change default `function scoped` event_loop behaviour."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


async def send_three_messages_with_delay(channel_name, channel_layer, delay):
    await channel_layer.send(channel_name, {'type': 'test.message', 'text': 'First!'})

    await asyncio.sleep(delay)

    await channel_layer.send(channel_name, {'type': 'test.message', 'text': 'Second!'})

    await asyncio.sleep(delay)

    await channel_layer.send(channel_name, {'type': 'test.message', 'text': 'Third!'})


async def group_send_three_messages_with_delay(group_name, channel_layer, delay):
    await channel_layer.group_send(
        group_name, {'type': 'test.message', 'text': 'First!'}
    )

    await asyncio.sleep(delay)

    await channel_layer.group_send(
        group_name, {'type': 'test.message', 'text': 'Second!'}
    )

    await asyncio.sleep(delay)

    await channel_layer.group_send(
        group_name, {'type': 'test.message', 'text': 'Third!'}
    )


@pytest.fixture()
async def channel_layer():
    """Channel layer fixture that flushes automatically."""
    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'])
    yield channel_layer
    await channel_layer.flush()


@pytest.mark.asyncio
async def test_send_receive_basic(channel_layer):
    """Makes sure we can send a message to a normal channel and receive it."""
    await channel_layer.send(
        'test-channel-1', {'type': 'test.message', 'text': 'Ahoy-hoy!'}
    )
    message = await channel_layer.receive('test-channel-1')

    assert message['type'] == 'test.message'
    assert message['text'] == 'Ahoy-hoy!'


@pytest.mark.asyncio
async def test_send_receive(channel_layer):
    """
    Similar to the `test_send_receive_basic`
    but mimics a real world scenario where clients connect first
    and wait for messages
    """

    task = create_task(channel_layer.receive('test-channel-2'))

    async def chained_tasks():
        await asyncio.sleep(1)
        message = await channel_layer.send(
            'test-channel-2', {'type': 'test.message_connect_wait', 'text': 'Hello world!'}
        )
        return message

    await asyncio.wait([task, create_task(chained_tasks())], timeout=2)

    message = task.result()
    assert message['type'] == 'test.message_connect_wait'
    assert message['text'] == 'Hello world!'


@pytest.mark.skip
# Skipped for now.... aiopg picks the first event loop it can find and sticks to it
@pytest.mark.parametrize("channel_layer", [None])  # Fixture can't handle sync
def test_double_receive(channel_layer):
    """
    Makes sure we can receive from two different event loops using
    process-local channel names.
    """
    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'])

    # aiopg connections can't be used from different event loops, so
    # send and close need to be done in the same async_to_sync call.
    async def send_and_close(*args, **kwargs):
        await channel_layer.send(*args, **kwargs)

    channel_name_1 = async_to_sync(channel_layer.new_channel)()
    channel_name_2 = async_to_sync(channel_layer.new_channel)()
    async_to_sync(send_and_close)(channel_name_1, {"type": "test.message.1"})
    async_to_sync(send_and_close)(channel_name_2, {"type": "test.message.2"})

    # Make things to listen on the loops
    async def listen1():
        message = await channel_layer.receive(channel_name_1)
        assert message["type"] == "test.message.1"

    async def listen2():
        message = await channel_layer.receive(channel_name_2)
        assert message["type"] == "test.message.2"

    # Run them inside threads
    async_to_sync(listen2)()
    async_to_sync(listen1)()
    # Clean up
    async_to_sync(channel_layer.flush)()


@pytest.mark.asyncio
async def test_process_local_send_receive(channel_layer):
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    channel_name = await channel_layer.new_channel()
    await channel_layer.send(
        channel_name, {"type": "test.message", "text": "Local only please"}
    )
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Local only please"


@pytest.mark.asyncio
async def test_multi_send_receive(channel_layer):
    """Tests overlapping sends and receives, and ordering."""
    await channel_layer.send("test-channel-3", {"type": "message.1"})
    await channel_layer.send("test-channel-3", {"type": "message.2"})
    await channel_layer.send("test-channel-3", {"type": "message.3"})
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.1"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.2"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.3"


@pytest.mark.asyncio
async def test_reject_bad_channel(channel_layer):
    """
    Makes sure sending/receiving on an invalic channel name fails.
    """
    with pytest.raises(TypeError):
        await channel_layer.send("=+135!", {"type": "foom"})
    with pytest.raises(TypeError):
        await channel_layer.receive("=+135!")


@pytest.mark.asyncio
async def test_reject_bad_client_prefix(channel_layer):
    """
    Makes sure receiving on a non-prefixed local channel is not allowed.
    """
    with pytest.raises(AssertionError):
        await channel_layer.receive("not-client-prefix!local_part")


@pytest.mark.asyncio
async def test_non_existent_group(channel_layer):
    """sending on Non-existent groups shoudldn't raise any Exceptions or send any message."""
    try:
        await channel_layer.group_send('non-existent', {'type': 'message.1'})
    except Exception as e:
        pytest.fail(f'Unexpected exception {e}')


@pytest.mark.asyncio
async def test_groups_basic(channel_layer):
    """
    Tests basic group operation.
    """
    channel_name1 = await channel_layer.new_channel(prefix="test-gr-chan-1")
    channel_name2 = await channel_layer.new_channel(prefix="test-gr-chan-2")
    channel_name3 = await channel_layer.new_channel(prefix="test-gr-chan-3")
    await channel_layer.group_add("test-group", channel_name1)
    await channel_layer.group_add("test-group", channel_name2)
    await channel_layer.group_add("test-group", channel_name3)
    await channel_layer.group_discard("test-group", channel_name2, expire=1)
    await channel_layer.group_send("test-group", {"type": "message.1"})
    # Make sure we get the message on the two channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name3))["type"] == "message.1"
    # Make sure the removed channel did not get the message
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name2)


@pytest.mark.asyncio
async def test_groups_same_prefix(channel_layer):
    """
    Tests group_send with multiple channels with same channel prefix
    """
    channel_name1 = await channel_layer.new_channel(prefix="test-gr-chan")
    channel_name2 = await channel_layer.new_channel(prefix="test-gr-chan")
    channel_name3 = await channel_layer.new_channel(prefix="test-gr-chan")
    await channel_layer.group_add("test-group", channel_name1)
    await channel_layer.group_add("test-group", channel_name2)
    await channel_layer.group_add("test-group", channel_name3)
    await channel_layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name2))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name3))["type"] == "message.1"


@pytest.mark.asyncio
async def test_receive_cancel(channel_layer):
    """
    Makes sure we can cancel a receive without blocking
    """
    channel = await channel_layer.new_channel()
    delay = 0
    while delay < 0.01:
        await channel_layer.send(channel, {"type": "test.message", "text": "Ahoy-hoy!"})

        task = asyncio.ensure_future(channel_layer.receive(channel))
        await asyncio.sleep(delay)
        task.cancel()
        delay += 0.0001

        try:
            await asyncio.wait_for(task, None)
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_random_reset__channel_name(channel_layer):
    """
    Makes sure resetting random seed does not make us reuse channel names.
    """
    random.seed(1)
    channel_name_1 = await channel_layer.new_channel()
    random.seed(1)
    channel_name_2 = await channel_layer.new_channel()

    assert channel_name_1 != channel_name_2


@pytest.mark.asyncio
async def test_random_reset__client_prefix():
    """
    Makes sure resetting random seed does not make us reuse client_prefixes.
    """
    random.seed(1)
    channel_layer_1 = PostgresChannelLayer(**settings.DATABASES['channels_postgres'])
    random.seed(1)
    channel_layer_2 = PostgresChannelLayer(**settings.DATABASES['channels_postgres'])
    assert channel_layer_1.client_prefix != channel_layer_2.client_prefix


@pytest.mark.asyncio
async def test_message_expiry__earliest_message_expires():
    expiry = 3
    delay = 2
    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'], expiry=expiry)
    channel_name = await channel_layer.new_channel()

    task = asyncio.ensure_future(
        send_three_messages_with_delay(channel_name, channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    # Make sure there's no third message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name)


@pytest.mark.asyncio
async def test_message_expiry__all_messages_under_expiration_time():
    expiry = 3
    delay = 1
    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'], expiry=expiry)
    channel_name = await channel_layer.new_channel()

    task = asyncio.ensure_future(
        send_three_messages_with_delay(channel_name, channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # expiry = 3, total delay under 3, all messages there
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "First!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"


@pytest.mark.asyncio
async def test_message_expiry__group_send():
    expiry = 3
    delay = 2
    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'], expiry=expiry)
    channel_name = await channel_layer.new_channel()

    await channel_layer.group_add("test-group", channel_name)

    task = asyncio.ensure_future(
        group_send_three_messages_with_delay("test-group", channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    # Make sure there's no third message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name)


@pytest.mark.asyncio
async def test_message_expiry__group_send__one_channel_expires_message():
    expiry = 4
    delay = 1

    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'], expiry=expiry)
    channel_1 = await channel_layer.new_channel()
    channel_2 = await channel_layer.new_channel(prefix="channel_2")

    await channel_layer.group_add("test-group", channel_1)
    await channel_layer.group_add("test-group", channel_2)

    # Let's give channel_1 one additional message and then sleep
    await channel_layer.send(channel_1, {"type": "test.message", "text": "Zero!"})
    await asyncio.sleep(2)

    task = asyncio.ensure_future(
        group_send_three_messages_with_delay("test-group", channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # message Zero! was sent about 2 + 1 + 1 seconds ago and it should have expired
    message = await channel_layer.receive(channel_1)
    assert message["type"] == "test.message"
    assert message["text"] == "First!"

    message = await channel_layer.receive(channel_1)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_1)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    # Make sure there's no fourth message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_1)

    # channel_2 should receive all three messages from group_send
    message = await channel_layer.receive(channel_2)
    assert message["type"] == "test.message"
    assert message["text"] == "First!"

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_2)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_2)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"


def test_default_group_key_format():
    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'])
    group_name = channel_layer._group_key("test_group")
    assert group_name == "asgi:group:test_group"


def test_custom_group_key_format():
    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'], prefix="test_prefix")
    group_name = channel_layer._group_key("test_group")
    assert group_name == "test_prefix:group:test_group"
