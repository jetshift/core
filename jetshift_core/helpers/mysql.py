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

    file_path = f'app/migrations/{table_name}.yaml'
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
    from config.logging import logger
    logger.error(f"MySQL connection failed: {str(error)}")


def fetch_and_extract_limit(self, engine, table_fields):
    import pandas as pd
    from jetshift_core.helpers.common import clear_files, create_data_directory
    from jetshift_core.helpers.clcikhouse import get_last_id_from_clickhouse, truncate_table as truncate_clickhouse_table

    table_name = self.table_name
    truncate_table = self.truncate_table
    output_path = self.output()['extracted'].path
    extract_offset = self.extract_offset
    extract_limit = self.extract_limit
    primary_id = self.primary_id

    if truncate_table is True:
        truncate_clickhouse_table(table_name)

    query = f"SELECT {', '.join(table_fields)} FROM {table_name}"

    if primary_id:
        last_id = get_last_id_from_clickhouse(table_name, primary_id)
        print(f'Last ClickHouse {table_name} {primary_id}: ', last_id)
        query += f" WHERE {primary_id} > {last_id}"

    query += f" LIMIT {extract_limit} OFFSET {extract_offset}"

    df = pd.read_sql(query, engine)

    clear_files(table_name)
    create_data_directory()
    df.to_csv(output_path, index=False)


def fetch_and_extract_chunk(self, engine, table_fields):
    import pandas as pd
    import time
    from jetshift_core.helpers.common import clear_files, create_data_directory
    from jetshift_core.helpers.clcikhouse import get_last_id_from_clickhouse, truncate_table as truncate_clickhouse_table

    table_name = self.table_name
    truncate_table = self.truncate_table
    output_path = self.output()['extracted'].path
    extract_offset = self.extract_offset
    extract_chunk_size = self.extract_chunk_size
    primary_id = self.primary_id
    sleep_interval = self.sleep_interval

    if truncate_table is True:
        truncate_clickhouse_table(table_name)

    clear_files(table_name)
    create_data_directory()

    if primary_id:
        last_id = get_last_id_from_clickhouse(table_name, primary_id)
        print(f'Last {table_name} {primary_id} (clickhouse): ', last_id)
        count_query = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_id} > {last_id}"
        base_query = f"SELECT {', '.join(table_fields)} FROM {table_name} WHERE {primary_id} > {last_id} LIMIT {extract_chunk_size}"
    else:
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        base_query = f"SELECT {', '.join(table_fields)} FROM {table_name} LIMIT {extract_chunk_size}"

    total_rows = pd.read_sql(count_query, engine).iloc[0, 0]
    if total_rows > 0:
        total_rows = total_rows - extract_offset
    print(f"Total rows in {table_name} (mysql): {total_rows}")

    loops = (total_rows + extract_chunk_size - 1) // extract_chunk_size
    print(f"Total loops: {loops}")

    print(f"\nExtracting data...")
    for i in range(loops):
        if i == 0:
            offset_query = f"{base_query} OFFSET {extract_offset}"
        else:
            offset_query = f"{base_query} OFFSET {(i * extract_chunk_size) + extract_offset}"

        df = pd.read_sql(offset_query, engine)
        df.to_csv(output_path, mode='a', header=(i == 0), index=False)

        print(f"Extracted {len(df)} rows from {table_name}. Loop {i + 1}/{loops}")
        time.sleep(sleep_interval)
