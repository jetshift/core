import argparse
from faker import Faker

from jetshift_core.helpers.mysql import mysql_connect

# Initialize Faker
fake = Faker()

# Database connection
connection = mysql_connect()

# Table name
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


def main(*args):
    parser = argparse.ArgumentParser(description="Seed with fake data.")
    parser.add_argument('-n', '--num', type=int, help="Number of records to seed", default=50)
    args = parser.parse_args(args)

    seed_table(args.num)  # Seed the specified number of records
    connection.close()
    print(f"Seeding completed. {args.num} records inserted.")


if __name__ == "__main__":
    main()
