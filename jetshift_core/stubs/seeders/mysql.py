import click
from faker import Faker
from jetshift_core.helpers.mysql import mysql_connect

fake = Faker()
connection = mysql_connect()
table_name = 'the_table_name'


def get_last_id():
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT MAX(id) FROM {table_name}")
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0


def seed_table(num_records):
    last_id = get_last_id()

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


@click.command()
@click.argument('records', required=False, default=10)
def main(records):
    seed_table(records)
    connection.close()
    print(f"Seeding completed. {records} records inserted to table {table_name}.")


if __name__ == "__main__":
    main()
