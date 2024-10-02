import datetime
import click
from faker import Faker
from config.logging import logger
from jetshift_core.helpers.mysql import mysql_connect, get_mysql_table_definition, get_last_id

fake = Faker()
connection = mysql_connect()


def generate_fake_data(table, fields):
    fake = Faker()
    formatted_row = []

    for field_name, field_type in fields:
        column = table.columns[field_name]
        if hasattr(column.type, 'length'):
            field_length = column.type.length
        else:
            field_length = None

        # inspector = inspect(table.metadata)
        # col_info = inspector.get_columns(table.name, column.name)[0]
        # print(col_info)

        if field_type == int and field_name != 'id':
            value = fake.random_int()
        elif field_type == str and field_name == 'name':
            value = fake.name()
        elif field_type == str and 'mail' in field_name:
            value = fake.email()
        elif field_type == str:
            value = fake.sentence(nb_words=6, variable_nb_words=True) if field_length is None else fake.text(max_nb_chars=field_length)
        elif field_type == float:
            value = fake.random_number(digits=5, fix_len=True) / 100.0
        elif field_type == datetime.datetime:
            value = fake.date_time_this_decade()
        else:
            value = None

        if value is not None:
            formatted_row.append(value)

    return tuple(formatted_row)


def seed_mysql(table_name, num_records):
    table = get_mysql_table_definition(table_name)
    fields = [(col.name, col.type.python_type) for col in table.columns]

    last_id = get_last_id(table_name)

    # fields = [(field[0], convert_field_to_python(field[1])) for field in fields_data]
    table_fields = [field[0] for field in fields]

    try:
        with connection.cursor() as cursor:
            inserted = 0
            primary_id = last_id + 1
            for i in range(1, num_records + 1):

                data = generate_fake_data(table, fields)
                data = (primary_id,) + data

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
