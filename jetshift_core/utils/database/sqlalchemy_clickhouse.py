from sqlalchemy import create_engine, MetaData, Table, Column, func
from sqlalchemy.exc import SQLAlchemyError
from jetshift_core.helpers.clcikhouse import get_clickhouse_credentials
from jetshift_core.helpers.common import jprint

# Initialize the SQLAlchemy metadata
metadata = MetaData()


def get_engine():
    try:
        host, user, password, database, port, secure = get_clickhouse_credentials()

        # HTTP protocol
        # engine = create_engine(f'clickhouse+http://{user}:{password}@{host}:{port}/{database}')

        # Native protocol
        engine = create_engine(f'clickhouse+native://{user}:{password}@{host}:{port}/{database}')

        return engine
    except SQLAlchemyError as e:
        jprint(f"ClickHouse SQLAlchemy error occurred: {e}", 'error')
    except Exception as e:
        jprint(f"An unexpected error occurred: {e}", 'error')


def create_table(table, fresh=False, drop=False):
    engine = get_engine()

    # Drop the table if it exists
    if fresh is True or drop is True:
        table.drop(engine, checkfirst=True)
        print(f"Dropped table: {table.name}")

    if engine is not None and drop is False:
        try:
            metadata.create_all(engine)
        except SQLAlchemyError as e:
            jprint(f"ClickHouse SQLAlchemy error during table creation: {e}", 'error')
        except Exception as e:
            jprint(f"An unexpected error occurred during ClickHouse table creation: {e}", 'error')
