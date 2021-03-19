import channels_postgres.models
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('channels_postgres', '0001_initial')
    ]

    setup_database_sql = """
            CREATE OR REPLACE FUNCTION delete_expired_messages() RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                DELETE FROM channels_postgres_message WHERE expire < NOW() - INTERVAL '1 minute';
                RETURN NEW;
                END;
                $$;

            DO $$ BEGIN
                CREATE TRIGGER delete_expired_messages_trigger
                    AFTER INSERT ON channels_postgres_message
                    EXECUTE PROCEDURE delete_expired_messages();
                EXCEPTION
                    WHEN others THEN null;
            END $$;

            CREATE OR REPLACE FUNCTION channels_postgres_notify()
                RETURNS trigger AS $$
                DECLARE
                BEGIN
                PERFORM pg_notify(NEW.channel, NEW.id::text);
                RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

            DO $$ BEGIN
                CREATE TRIGGER channels_postgres_notify_trigger
                    AFTER INSERT ON channels_postgres_message
                    FOR EACH ROW
                    EXECUTE PROCEDURE channels_postgres_notify();
                EXCEPTION
                    WHEN others THEN null;
            END $$;
        """

    reverse_setup_database_sql = """
            DROP TABLE channels_postgres_groupchannel CASCADE;
            DROP TABLE channels_postgres_message CASCADE;
        """

    operations = [
        migrations.RunSQL(
            sql=setup_database_sql, reverse_sql=reverse_setup_database_sql
        )
    ]
