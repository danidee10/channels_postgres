from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [('channels_postgres', '0002_create_triggers_and_functions')]

    # For messages than are smaller than 7168 bytes, we can send the whole message in the payload
    # Otherwise, we send only the message id

    # Postgres' NOTIFY messages are limited to 8000 bytes, so we can't always send the whole message
    # in the payload.
    setup_database_sql = """
        CREATE OR REPLACE FUNCTION channels_postgres_notify()
        RETURNS trigger AS $$
        DECLARE
            payload text;
        BEGIN
            IF octet_length(NEW.message) <= 7168 THEN
                payload := NEW.id::text || ':' || NEW.channel::text || ':' || encode(NEW.message, 'base64') || ':' || extract(epoch from NEW.expire)::text;
            ELSE
                payload := NEW.id::text || ':' || NEW.channel::text;
            END IF;

            PERFORM pg_notify('channels_postgres_message', payload);
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
        DROP FUNCTION IF EXISTS channels_postgres_notify;
        DROP TRIGGER IF EXISTS channels_postgres_notify_trigger ON channels_postgres_message;
    """  # noqa

    operations = [
        migrations.RunSQL(sql=setup_database_sql, reverse_sql=reverse_setup_database_sql),
    ]
