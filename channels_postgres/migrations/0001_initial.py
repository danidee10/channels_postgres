from django.db import migrations, models

import channels_postgres.models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='GroupChannel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),  # noqa
                ('group_key', models.CharField(max_length=100)),
                ('channel', models.CharField(max_length=100)),
                ('expire', models.DateTimeField(default=channels_postgres.models._default_channel_expiry_time))  # noqa
            ],
        ),
        migrations.CreateModel(
            name='Message',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),  # noqa
                ('channel', models.CharField(max_length=100)),
                ('message', models.BinaryField(max_length=1000)),
                ('expire', models.DateTimeField(default=channels_postgres.models._default_message_expiry_time)),  # noqa
            ],
        )
    ]
