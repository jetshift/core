import click
from config.logging import logger
from jetshift_core.commands.migrations.common import generate_fake_data
from jetshift_core.helpers.mysql import mysql_connect, get_mysql_table_definition, get_last_id


def seed_mysql(engine, table_name, num_records):
    table = get_mysql_table_definition(table_name)
    fields = [(col.name, col.type.python_type) for col in table.columns]

    last_id = get_last_id(table_name)

    # fields = [(field[0], convert_field_to_python(field[1])) for field in fields_data]
    table_fields = [field[0] for field in fields]

    try:
        connection = mysql_connect()
        with connection.cursor() as cursor:
            inserted = 0
            primary_id = last_id + 1
            for i in range(1, num_records + 1):

                data = generate_fake_data(engine, table, fields)
                data = (primary_id,) + data

                # print(data)

                placeholders = ', '.join(['%s'] * len(table_fields))
                mysql_query = f"INSERT INTO {table_name} ({', '.join(table_fields)}) VALUES ({placeholders})"
                cursor.execute(mysql_query, data)

                primary_id += 1
                inserted += 1

                if inserted % 10000 == 0:
                    click.echo(f"Inserted {inserted} records. Remaining: {num_records - i}")

            click.echo(f"Seeded {inserted} records in the table: {table_name}")

        connection.commit()
    except Exception as e:
        logger.error("An error occurred while seeding the table: %s", e)
