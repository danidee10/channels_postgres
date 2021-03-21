from django.db import models
from django.utils import timezone


def _default_channel_expiry_time():
    return timezone.now() + timezone.timedelta(seconds=86400)


def _default_message_expiry_time():
    return timezone.now() + timezone.timedelta(minutes=1)


class GroupChannel(models.Model):
    group_key = models.CharField(max_length=100, null=False)
    channel = models.CharField(max_length=100, null=False)
    expire = models.DateTimeField(default=_default_channel_expiry_time)


class Message(models.Model):
    channel = models.CharField(max_length=100)
    message = models.BinaryField(max_length=1000)
    expire = models.DateTimeField(default=_default_message_expiry_time)
