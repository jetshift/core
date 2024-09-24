import click
from faker import Faker
from config.logging import logger
from jetshift_core.helpers.mysql import mysql_connect, get_last_id, get_min_max_id

fake = Faker()
connection = mysql_connect()
table_name = 'the_table_name'


def seed_table(num_records):
    last_id = get_last_id(table_name)

    try:
        with connection.cursor() as cursor:
            for i in range(1, num_records + 1):
                id = last_id + i
                name = fake.name()
                created_at = fake.date_time_this_decade()

                sql = f"""
                INSERT INTO {table_name} (id, name, created_at)
                VALUES (%s, %s, %s)
                """
                cursor.execute(sql, (id, name, created_at))

        connection.commit()
    except Exception as e:
        logger.error("An error occurred while seeding the table: %s", e)


@click.command()
@click.argument('records', required=False, default=10)
def main(records):
    seed_table(records)
    connection.close()
    print(f"Seeding completed. {records} records inserted to table {table_name}.")


if __name__ == "__main__":
    main()
