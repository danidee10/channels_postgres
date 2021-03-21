import asyncio

import django
from django.conf import settings

import pytest
import async_timeout
from asgiref.sync import async_to_sync
from async_generator import async_generator, yield_

django.setup()  # noqa
from channels_postgres.core import PostgresChannelLayer  # noqa


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
@async_generator
async def channel_layer():
    """Channel layer fixture that flushes automatically."""
    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'])
    await yield_(channel_layer)
    # await channel_layer.flush()


@pytest.mark.asyncio
async def test_send_receive(channel_layer):
    """Makes sure we can send a message to a normal channel and receive it."""
    await channel_layer.send(
        'test-channel-1', {'type': 'test.message', 'text': 'Ahoy-hoy!'}
    )
    message = await channel_layer.receive('test-channel-1')

    assert message['type'] == 'test.message'
    assert message['text'] == 'Ahoy-hoy!'


@pytest.mark.skip
# Skipped for now....
@pytest.mark.parametrize("channel_layer", [None])  # Fixture can't handle sync
def test_double_receive(channel_layer):
    """
    Makes sure we can receive from two different event loops using
    process-local channel names.
    """
    channel_layer = PostgresChannelLayer(**settings.DATABASES['channels_postgres'])

    # Aioredis connections can't be used from different event loops, so
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
    await channel_layer.group_discard("test-group", channel_name2)
    await channel_layer.group_send("test-group", {"type": "message.1"})
    # Make sure we get the message on the two channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name3))["type"] == "message.1"
    # Make sure the removed channel did not get the message
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name2)
