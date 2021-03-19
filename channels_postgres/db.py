"""common db methods."""

from asgiref.sync import sync_to_async
from channels_postgres.models import GroupChannel, Message


class DatabaseLayer:
    def __init__(self, using='channels_postgres'):
        self.using = using

    @sync_to_async
    def send_on_channel(self, channel, message):
        """Send a message on a channel."""
        Message.objects.using(self.using).create(
            channel=channel, message=message
        )

    @sync_to_async
    def add_channel_to_group(self, group_key, channel):
        GroupChannel.objects.using(self.using).create(
            group_key=group_key, channel=channel
        )

        print(f'Channel {channel} added to Group {group_key}')

    @sync_to_async
    def retrieve_group_channels(self, group_key):
        return GroupChannel.objects.using('channels_postgres').filter(
            group_key=group_key
        ).distinct('channel')

