from datetime import timedelta

from django.db import models
from django.utils import timezone


def _default_time():
        return timezone.now() + timedelta(minutes=1)


class GroupChannel(models.Model):
    group_key = models.CharField(max_length=100, null=False)
    channel = models.CharField(max_length=100, null=False)


class Message(models.Model):
    channel = models.CharField(max_length=100)
    message = models.BinaryField(max_length=1000)
    expire = models.DateTimeField(default=_default_time)
