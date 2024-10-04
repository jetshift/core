import click
from config.logging import logger
from jetshift_core.commands.migrations.common import generate_fake_data
from jetshift_core.commands.seeders.common import find_dependencies
from jetshift_core.helpers.mysql import mysql_connect, get_mysql_table_definition, get_last_id


def seed_mysql(engine, table_name, num_records):
    try:
        # seed(engine, table_name, num_records)

        tables = find_dependencies(engine, table_name)
        reversed_dependency_order = dict(reversed(tables.items()))

        for the_table_name, details in reversed_dependency_order.items():
            seed(engine, the_table_name, num_records)

    except Exception as e:
        logger.error("%s", e)


def seed(engine, table_name, num_records):
    try:
        table = get_mysql_table_definition(table_name)
        fields = [(col.name, col.type.python_type) for col in table.columns]

        # from csv
        data_info = table.info.get('data', False)
        if data_info is True:
            fields, data = generate_fake_data(engine, table, fields)

            inset(table_name, fields, data)

            click.echo(f"Seeded {len(data)} records in the table: {table_name}")

            return True

        # generate fake data
        # fields = [(field[0], convert_field_to_python(field[1])) for field in fields_data]
        table_fields = [field[0] for field in fields]
        last_id = get_last_id(table_name)

        data = []
        inserted = 0
        primary_id = last_id + 1
        for i in range(1, num_records + 1):

            row = generate_fake_data(engine, table, fields)
            row = (primary_id,) + row
            # print(data)
            data.append(row)

            primary_id += 1
            inserted += 1

            if inserted % 10000 == 0:
                click.echo(f"Inserted {inserted} records. Remaining: {num_records - i}")

        inset(table_name, table_fields, data)

        click.echo(f"Seeded {inserted} records in the table: {table_name}")

    except Exception as e:
        logger.error("An error occurred while seeding the table: %s", e)


def inset(table_name, fields, data):
    try:
        connection = mysql_connect()
        cursor = connection.cursor()
        placeholders = ', '.join(['%s'] * len(fields))
        mysql_query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
        cursor.executemany(mysql_query, data)
        connection.commit()
    except Exception as e:
        logger.error("An error occurred while seeding the table: %s", e)
