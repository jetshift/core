from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, DECIMAL, DateTime, func
from sqlalchemy.exc import SQLAlchemyError
from jetshift_core.helpers.mysql import get_mysql_credentials

# Initialize the SQLAlchemy metadata
metadata = MetaData()


def get_engine():
    from jetshift_core.helpers.common import jprint
    try:
        host, user, password, database = get_mysql_credentials()

        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        return engine
    except SQLAlchemyError as e:
        jprint(f"MySQL SQLAlchemy error occurred: {e}", 'error')
    except Exception as e:
        jprint(f"An unexpected error occurred: {e}", 'error')


def create_table(table, fresh=False, drop=False):
    from jetshift_core.helpers.common import jprint
    engine = get_engine()

    # Drop the table if it exists
    if fresh is True or drop is True:
        table.drop(engine, checkfirst=True)
        print(f"Dropped table: {table.name}")

    if engine is not None and drop is False:
        try:
            metadata.create_all(engine)
        except SQLAlchemyError as e:
            jprint(f"MySQL SQLAlchemy error during table '{table.name}' creation: {e}", 'error')
        except Exception as e:
            jprint(f"An unexpected error occurred during MySQL table '{table.name}' creation: {e}", 'error')
