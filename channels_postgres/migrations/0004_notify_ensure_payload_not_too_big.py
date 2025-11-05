from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [('channels_postgres', '0003_notify_smaller_payload')]

    # For messages than are smaller than 7168 bytes, we can send the whole message in the payload
    # Otherwise, we send only the message id

    # Postgres' NOTIFY messages are limited to 8000 bytes, so we can't always send the whole message
    # in the payload.
    
    # +3 is added to 8000 bytes comparison to account for the three colon separators in id:channel:encoded_message:epoch
    setup_database_sql = """
        CREATE OR REPLACE FUNCTION channels_postgres_notify()
        RETURNS trigger AS $$
        DECLARE
            payload text;
            encoded_message text;
            epoch text;
        BEGIN
            encoded_message := encode(NEW.message, 'base64');
            epoch := extract(epoch from NEW.expire)::text;
            IF octet_length(NEW.id::text) + octet_length(NEW.channel::text) + octet_length(encoded_message) + octet_length(epoch) + 3 <= 8000 THEN
                payload := NEW.id::text || ':' || NEW.channel::text || ':' || encoded_message || ':' || epoch;
            ELSE
                payload := NEW.id::text || ':' || NEW.channel::text;
            END IF;

            PERFORM pg_notify('channels_postgres_messages', payload);
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
        DROP TRIGGER IF EXISTS channels_postgres_notify_trigger ON channels_postgres_message;
        DROP FUNCTION IF EXISTS channels_postgres_notify;
    """  # noqa

    operations = [
        migrations.RunSQL(sql=setup_database_sql, reverse_sql=reverse_setup_database_sql),
    ]
