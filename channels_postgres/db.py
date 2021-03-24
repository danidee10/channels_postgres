"""common db methods."""

import logging

from django.utils import timezone

from channels.db import database_sync_to_async
from channels_postgres.models import GroupChannel, Message


class DatabaseLayer:
    def __init__(self, using='channels_postgres', logger=None):
        self.using = using
        self.logger = logger

        if not self.logger:
            self.logger = logging.getLogger('channels_postgres.database')

    @database_sync_to_async
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

    @database_sync_to_async
    def add_channel_to_group(self, group_key, channel, expire):
        GroupChannel.objects.using(self.using).create(
            group_key=group_key, channel=channel,
            expire=timezone.now() + timezone.timedelta(seconds=expire)
        )

        self.logger.debug('Channel %s added to Group %s', channel, group_key)

    @database_sync_to_async
    def retrieve_group_channels(self, group_key):
        return GroupChannel.objects.using('channels_postgres').filter(
            group_key=group_key
        ).distinct('channel').values_list('channel', flat=True)

    @database_sync_to_async
    def delete_expired_groups(self):
        GroupChannel.objects.using(self.using).filter(
            expire__lt=timezone.now()
        ).delete()

    @database_sync_to_async
    def delete_expired_messages(self):
        Message.objects.using(self.using).filter(
            expire__lt=timezone.now()
        ).delete()
