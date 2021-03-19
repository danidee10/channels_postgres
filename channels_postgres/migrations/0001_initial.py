import channels_postgres.models
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='GroupChannel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('group_key', models.CharField(max_length=100)),
                ('channel', models.CharField(max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='Message',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('channel', models.CharField(max_length=100)),
                ('message', models.BinaryField(max_length=1000)),
                ('expire', models.DateTimeField(default=channels_postgres.models._default_time)),
            ],
        )
    ]
