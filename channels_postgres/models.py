"""channels_postgres models"""

from datetime import datetime

from django.db import models
from django.utils import timezone


def _default_channel_expiry_time() -> datetime:
    return timezone.now() + timezone.timedelta(seconds=86400)


def _default_message_expiry_time() -> datetime:
    return timezone.now() + timezone.timedelta(minutes=1)


class GroupChannel(models.Model):
    """
    A model that represents a group channel.

    Groups are used to send messages to multiple channels.
    """

    group_key = models.CharField(max_length=100, null=False)
    channel = models.CharField(max_length=100, null=False)
    expire = models.DateTimeField(default=_default_channel_expiry_time)


class Message(models.Model):
    """
    A model that represents a message.

    Messages are used to send messages to a specific channel.
    E.g for user to user private messages.
    """

    channel = models.CharField(max_length=100)
    message = models.BinaryField(max_length=1000)
    expire = models.DateTimeField(default=_default_message_expiry_time)
