"""common db methods."""

from django.utils import timezone

from asgiref.sync import sync_to_async
from channels_postgres.models import GroupChannel, Message


class DatabaseLayer:
    def __init__(self, using='channels_postgres'):
        self.using = using

    @sync_to_async
    def send_to_channel(self, channels, message, expire):
        """Send a message on a channel."""
        messages = []
        for channel in channels:
            message_obj = Message(
                channel=channel, message=message,
                expire=timezone.now() + timezone.timedelta(seconds=expire)
            )
            messages.append(message_obj)

        Message.objects.using(self.using).bulk_create(messages)

    @sync_to_async
    def add_channel_to_group(self, group_key, channel, expire):
        GroupChannel.objects.using(self.using).create(
            group_key=group_key, channel=channel,
            expire=timezone.now() + timezone.timedelta(seconds=expire)
        )

        print(f'Channel {channel} added to Group {group_key}')

    @sync_to_async
    def retrieve_group_channels(self, group_key):
        return GroupChannel.objects.using('channels_postgres').filter(
            group_key=group_key
        ).distinct('channel').values_list('channel', flat=True)

    @sync_to_async
    def delete_expired_groups(self, expire):
        GroupChannel.objects.using(self.using).filter(
            expire__lt=timezone.now() - timezone.timedelta(seconds=expire)
        ).delete()

    @sync_to_async
    def delete_expired_messages(self, expire):
        Message.objects.using(self.using).filter(
            expire__lt=timezone.now() - timezone.timedelta(seconds=expire)
        ).delete()
