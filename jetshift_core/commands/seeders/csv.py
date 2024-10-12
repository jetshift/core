import os
import click
from config.logging import logger
import pandas as pd
from jetshift_core.commands.migrations.common import generate_fake_data
from jetshift_core.commands.seeders.common import find_dependencies, table_has_data
from jetshift_core.helpers.common import create_data_directory
from jetshift_core.helpers.mysql import get_mysql_table_definition


def seed_csv(engine, table_name, num_records, dependent_records, skip_dependencies, skip_dependencies_if_data_exists):
    try:
        if skip_dependencies is False:
            tables = find_dependencies(engine, table_name, dependent_records)
            reversed_dependency_order = dict(reversed(tables.items()))

            for index, (the_table_name, details) in enumerate(reversed_dependency_order.items()):
                if index == len(reversed_dependency_order) - 1:
                    seed(engine, table_name, num_records)
                else:
                    seed(engine, the_table_name, details['dependent_records'], skip_dependencies_if_data_exists)
        else:
            seed(engine, table_name, num_records)
    except Exception as e:
        logger.error("%s", e)


def seed(engine, table_name, num_records, skip_if_data_exists=False):
    try:
        table = get_mysql_table_definition(table_name)
        fields = [(col.name, col.type.python_type) for col in table.columns]

        # check if data is available in the table
        check_table_has_data = table_has_data(engine, table_name)
        if check_table_has_data is True and skip_if_data_exists is True:
            return True

        # generate fake data
        table_fields = [field[0] for field in fields]
        # last_id = get_last_inserted_id(table_name)
        last_id = 0

        data = []
        inserted = 0
        primary_id = last_id + 1
        for i in range(1, num_records + 1):

            row = generate_fake_data(engine, table, fields)
            row = (primary_id,) + row
            data.append(row)

            primary_id += 1
            inserted += 1

            if inserted % 10000 == 0:
                success = insert_into_csv(table_name, table_fields, data)
                if success:
                    data = []
                    click.echo(f"Seeded {inserted} records. Remaining: {num_records - i}")

        success = insert_into_csv(table_name, table_fields, data)
        if success:
            click.echo(f"Seeded {inserted} records in the file: {table_name}.csv.")
    except Exception as e:
        logger.error("An error occurred while seeding the table: %s", e)


def insert_into_csv(table_name, table_fields, data):
    # Ensure 'data' directory exists
    output_dir = 'data'
    create_data_directory(output_dir)

    df = pd.DataFrame(data, columns=table_fields)

    # Save DataFrame to CSV in the 'data' directory
    csv_file_name = os.path.join(output_dir, f"{table_name}.csv")
    df.to_csv(csv_file_name, index=False)

    return True,


def get_last_inserted_id(table_name):
    # Ensure 'data' directory exists
    output_dir = 'data'
    create_data_directory(output_dir)

    # Load the CSV file
    csv_file_name = f"{output_dir}/{table_name}.csv"

    # Check if the file exists and has content
    if not os.path.exists(csv_file_name):
        return 0

    try:
        # Read the CSV file and get the last row
        df = pd.read_csv(csv_file_name)

        if df.empty or 'id' not in df.columns:
            return 0

        # Extract the 'id' from the last row
        last_row = df.tail(1)
        last_id = last_row['id'].values[0]

        return last_id
    except pd.errors.EmptyDataError:
        # Handle case where CSV is empty
        return 0
