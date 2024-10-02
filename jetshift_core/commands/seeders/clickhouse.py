import click
from config.logging import logger

from jetshift_core.commands.migrations.common import generate_fake_data
from jetshift_core.helpers.clcikhouse import insert_into_clickhouse, get_last_id_from_clickhouse
from jetshift_core.helpers.mysql import get_mysql_table_definition


def seed_clickhouse(engine, table_name, num_records):
    table = get_mysql_table_definition(table_name)
    fields = [(col.name, col.type.python_type) for col in table.columns]

    last_id = get_last_id_from_clickhouse(table_name)

    table_fields = [field[0] for field in fields]

    try:
        formatted_data = []
        inserted = 0
        primary_id = last_id + 1
        for i in range(1, num_records + 1):

            data = generate_fake_data(engine, table, fields)
            data = (primary_id,) + data

            formatted_data.append(data)

            primary_id += 1
            inserted += 1

            if inserted % 10000 == 0:
                click.echo(f"Inserted {inserted} records. Remaining: {num_records - i}")

        success, last_inserted_id = insert_into_clickhouse(table_name, table_fields, formatted_data)
        if success:
            click.echo(f"Seeded {inserted} records in the table: {table_name}. Last inserted ID: {last_inserted_id}")
    except Exception as e:
        logger.error("An error occurred while seeding the table: %s", e)
