def get_mysql_credentials():
    from config.database import mysql
    credentials = mysql()
    return credentials['host'], credentials['user'], credentials['password'], credentials['database']


def mysql_client(test_query=None):
    from sqlalchemy import create_engine
    host, user, password, database = get_mysql_credentials()

    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}/{database}"
        engine = create_engine(
            connection_string,
            pool_recycle=3600,
            connect_args={"connect_timeout": 3}  # Timeout set to 3 seconds
        )
        if test_query:
            with engine.connect() as conn:
                result = conn.execute(test_query)
                return result.fetchone()
        else:
            return engine
    except Exception as e:
        handle_mysql_error(e)


def mysql_connect():
    try:
        engine = mysql_client()
        if isinstance(engine, dict):
            return 'Connection failed'

        return engine.raw_connection()
    except Exception as e:
        handle_mysql_error(e)


def get_last_id(table_name, column_name='id'):
    try:
        connection = mysql_connect()
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT MAX({column_name}) FROM {table_name}")
            result = cursor.fetchone()
            return result[0] if result[0] is not None else 0
    except Exception as e:
        handle_mysql_error(e)


def get_min_max_id(table_name):
    try:
        connection = mysql_connect()
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT MIN(id), MAX(id) FROM {table_name}")
            result = cursor.fetchone()
            if result[0] is None or result[1] is None:
                return 0, 0
            return result[0], result[1]
    except Exception as e:
        handle_mysql_error(e)


def handle_mysql_error(error):
    from config.logging import logger
    logger.error(f"MySQL connection failed: {str(error)}")
