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


def get_mysql_table_definition(table_name, live_schema=False):
    if live_schema is True:
        table = get_mysql_database_table_definition(table_name)
    else:
        table = get_mysql_yaml_table_definition(table_name)

    return table


# Reflect database structure from yaml file
def get_mysql_yaml_table_definition(table_name):
    import os
    import sys
    from jetshift_core.helpers.common import jprint
    from jetshift_core.commands.migrations.mysql import yaml_table_definition

    app_path = os.environ.get('APP_PATH', '')
    file_path = f'{app_path}app/migrations/{table_name}.yml'
    if not os.path.exists(file_path):
        jprint(f"Migration '{file_path}' does not exist.", 'error')
        sys.exit(1)

    table = yaml_table_definition(file_path)

    return table


# Reflect the existing database structure
def get_mysql_database_table_definition(table_name):
    from jetshift_core.utils.database.sqlalchemy_mysql import get_engine, MetaData, Table

    engine = get_engine()
    metadata = MetaData()
    metadata.reflect(bind=engine)

    # Access the users table using the reflected metadata
    table = Table(table_name, metadata, autoload_with=engine)

    return table


def check_table_has_data(table_name):
    try:
        connection = mysql_connect()
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            result = cursor.fetchone()
            return result is not None
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
    from jetshift_core.js_logger import get_logger
    logger = get_logger(__name__)
    logger.error(f"MySQL connection failed: {str(error)}")


def fetch_and_extract_limit(params):
    import pandas as pd
    from sqlalchemy import MetaData, Table, select
    from jetshift_core.helpers.common import clear_files, create_data_directory
    from jetshift_core.helpers.clcikhouse import get_last_id_from_clickhouse, truncate_table as truncate_clickhouse_table

    table_name = params.source_table
    truncate_table = params.truncate_table
    output_path = params.output_path
    extract_offset = params.extract_offset
    extract_limit = params.extract_limit
    primary_id = params.primary_id

    if truncate_table:
        truncate_clickhouse_table(params.target_engine, table_name)

    # Reflect the table structure
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=params.source_engine)

    # Start building the SQLAlchemy query
    stmt = select(table)

    # If primary_id is defined, apply the last_id filtering
    if primary_id:
        last_id = get_last_id_from_clickhouse(params.target_engine, table_name, primary_id)
        print(f'Last ClickHouse {table_name} {primary_id}: ', last_id)
        stmt = stmt.where(table.c[primary_id] > last_id)

    # Apply limit and offset
    stmt = stmt.limit(extract_limit).offset(extract_offset)

    # Use pandas to execute and fetch the query result
    df = pd.read_sql(stmt, params.source_engine)

    # Clear old files and save new CSV
    clear_files(table_name)
    create_data_directory()
    df.to_csv(output_path, index=False)


def fetch_and_extract_chunk(params):
    import pandas as pd
    import time
    from sqlalchemy import MetaData, Table, select, func
    from jetshift_core.helpers.common import clear_files, create_data_directory
    from jetshift_core.helpers.clcikhouse import get_last_id_from_clickhouse, truncate_table as truncate_clickhouse_table

    table_name = params.source_table
    truncate_table = params.truncate_table
    output_path = params.output_path
    extract_offset = params.extract_offset
    extract_chunk_size = params.extract_chunk_size
    primary_id = params.primary_id
    sleep_interval = params.sleep_interval

    if truncate_table:
        truncate_clickhouse_table(params.target_engine, table_name)

    clear_files(table_name)
    create_data_directory()

    # Reflect the table
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=params.source_engine)

    # Build count query
    stmt_count = select(func.count()).select_from(table)
    last_id = None
    if primary_id:
        last_id = get_last_id_from_clickhouse(params.target_engine, table_name, primary_id)
        print(f'Last {table_name} {primary_id} (clickhouse): ', last_id)
        stmt_count = stmt_count.where(table.c[primary_id] > last_id)

    # Execute count properly
    with params.source_engine.connect() as connection:
        total_rows = connection.execute(stmt_count).scalar()

    if total_rows > 0:
        total_rows -= extract_offset
    print(f"Total rows in {table_name} (mysql): {total_rows}")

    loops = (total_rows + extract_chunk_size - 1) // extract_chunk_size
    print(f"Total loops: {loops}")
    print(f"\nExtracting data...")

    # Extract in chunks
    for i in range(loops):
        stmt = select(table)

        if primary_id and last_id is not None:
            stmt = stmt.where(table.c[primary_id] > last_id)

        # Apply limit and dynamic offset for each loop
        current_offset = (i * extract_chunk_size) + extract_offset
        stmt = stmt.limit(extract_chunk_size).offset(current_offset)

        # Read data into DataFrame
        df = pd.read_sql(stmt, params.source_engine)

        # Append chunk to CSV
        df.to_csv(output_path, mode='a', header=(i == 0), index=False)

        print(f"Extracted {len(df)} rows from {table_name}. Loop {i + 1}/{loops}")
        time.sleep(sleep_interval)
