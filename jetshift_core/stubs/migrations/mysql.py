from jetshift_core.utils.database.sqlalchemy_mysql import *
from sqlalchemy.dialects.mysql import INTEGER

# Define table
table = Table(
    'table_name', metadata,
    Column('id', INTEGER(unsigned=True), primary_key=True, autoincrement=True),
    Column('created_at', DateTime, nullable=False, server_default=func.now()),
    Column('updated_at', DateTime, nullable=True, server_default=None)
)


def main(fresh=None):
    create_table(table, fresh)


if __name__ == "__main__":
    main()
