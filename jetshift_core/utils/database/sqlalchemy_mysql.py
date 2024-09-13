from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, DECIMAL, DateTime, func
from sqlalchemy.exc import SQLAlchemyError
from jetshift_core.helpers.mysql import get_mysql_credentials

# Initialize the SQLAlchemy metadata
metadata = MetaData()

def get_engine():
    try:
        host, user, password, database = get_mysql_credentials()

        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        return engine
    except SQLAlchemyError as e:
        print(f"MySQL SQLAlchemy error occurred: {e}")
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
            print("MySQL table created successfully!")
        except SQLAlchemyError as e:
            print(f"MySQL SQLAlchemy error during table creation: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during MySQL table creation: {e}")
