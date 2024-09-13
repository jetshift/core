def get_mysql_credentials():
    from config.database import mysql
    credentials = mysql()
    return credentials['host'], credentials['user'], credentials['password'], credentials['database']


def mysql_connect():
    import pymysql

    host, user, password, database = get_mysql_credentials()

    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return connection
    except pymysql.MySQLError as e:
        handle_mysql_error(e)


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


def handle_mysql_error(error):
    from jetshift_core.helpers.common import send_discord_message

    error_message = f"MySQL connection failed: {str(error)}"
    print(error_message)
    send_discord_message(error_message)

    return {
        'statusCode': 500,
        'message': error_message
    }
