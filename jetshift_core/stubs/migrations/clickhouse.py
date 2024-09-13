from jetshift_core.utils.database.sqlalchemy_clickhouse import *
from clickhouse_sqlalchemy import types, engines

# Define table
table = Table(
    'table_name', metadata,
    Column('id', types.UInt32, primary_key=True, autoincrement=True),
    Column('created_at', types.DateTime, nullable=False, server_default=func.now()),
    Column('updated_at', types.DateTime, nullable=True, server_default=None)
    ,
    engines.MergeTree(order_by=['id'])  # Specify the MergeTree engine with ORDER BY clause
)


def main():
    create_table(table)


if __name__ == "__main__":
    main()
