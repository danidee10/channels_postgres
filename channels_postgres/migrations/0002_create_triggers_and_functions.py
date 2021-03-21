from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('channels_postgres', '0001_initial')
    ]

    setup_database_sql = """
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
        DROP FUNCTION IF EXISTS channels_postgres_notify;
        DROP TRIGGER IF EXISTS channels_postgres_notify_trigger ON channels_postgres_message;
    """  # noqa

    operations = [
        migrations.RunSQL(
            sql=setup_database_sql, reverse_sql=reverse_setup_database_sql
        )
    ]
