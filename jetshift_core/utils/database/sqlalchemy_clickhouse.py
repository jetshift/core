from sqlalchemy import create_engine, MetaData, Table, Column, func
from sqlalchemy.exc import SQLAlchemyError
from jetshift_core.helpers.clcikhouse import get_clickhouse_credentials

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
        print(f"ClickHouse SQLAlchemy error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def create_table(table):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("f", nargs='?', default=None, help="Truncate table during migration")
    args = parser.parse_args()
    engine = get_engine()

    # Drop the table if it exists
    if args.f:
        table.drop(engine, checkfirst=True)

    if engine is not None:
        try:
            metadata.create_all(engine)
            print("ClickHouse table created successfully!")
        except SQLAlchemyError as e:
            print(f"ClickHouse SQLAlchemy error during table creation: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during ClickHouse table creation: {e}")
