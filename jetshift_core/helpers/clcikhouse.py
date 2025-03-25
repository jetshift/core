from clickhouse_driver import Client
from clickhouse_driver.errors import Error


# Map ClickHouse types to pandas dtypes
def get_clickhouse_to_pandas_type(ch_type):
    ch_to_pd_type = {
        'UInt64': 'Int64',
        'UInt32': 'Int64',
        'Int32': 'Int64',
        'Int64': 'Int64',
        'String': 'string',
        'Nullable(String)': 'string',
        'DateTime': 'datetime64[ns]',
        'Nullable(DateTime)': 'datetime64[ns]',
        'Date': 'datetime64[ns]',
        'Nullable(Date)': 'datetime64[ns]',
        'Float32': 'float',
        'Nullable(Float32)': 'float',
        'Float64': 'float',
        'Nullable(Float64)': 'float',
    }
    return ch_to_pd_type.get(ch_type, 'string')  # default to 'string' if unknown


def get_clickhouse_credentials():
    from config.database import clickhouse
    credentials = clickhouse()

    return credentials['host'], credentials['user'], credentials['password'], credentials['database'], credentials['port'], credentials['secure']


def clickhouse_client():
    try:
        host, user, password, database, port, secure = get_clickhouse_credentials()
        client = Client(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            connect_timeout=3,
            secure=secure
        )
        return client
    except Error as e:
        from jetshift_core.helpers.common import send_discord_message  # Local import

        # Extract the first 5 lines of the error message
        error_lines = str(e).split('\n')[:5]  # Get the first 5 lines
        main_error_message = '\n'.join(error_lines)  # Join them back into a string

        # Construct the error message to send
        error_message = f"ClickHouse connection failed:\n{main_error_message}"

        print(error_message)
        send_discord_message(error_message)

        return {
            'statusCode': 500,
            'message': error_message
        }


def ping_clickhouse():
    from jetshift_core.helpers.common import send_discord_message

    try:
        clickhouse = clickhouse_client()
        clickhouse.execute(f"SELECT 1")
        clickhouse.disconnect_connection()

        error_message = f"ClickHouse ping successful!"
        send_discord_message(error_message)
        return {
            'success': True,
            'message': 'ClickHouse ping successful!'
        }
    except Error as e:
        error_message = f"ClickHouse ping failed: {str(e)}"
        send_discord_message(error_message)

        return {
            'success': False,
            'message': error_message
        }


def check_table_has_data(table_name):
    from jetshift_core.js_logger import get_logger
    logger = get_logger(__name__)

    try:
        with clickhouse_client() as clickhouse:
            result = clickhouse.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return len(result) > 0
    except Exception as e:
        logger.error("Failed to check data for table '%s': %s", table_name, e)
        return False


def get_last_id_from_clickhouse(target_engine, table_name, primary_id='id'):
    from sqlalchemy import MetaData, Table, select, desc

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=target_engine)

    stmt = select(table.c[primary_id]).order_by(desc(table.c[primary_id])).limit(1)

    with target_engine.connect() as connection:
        result = connection.execute(stmt).fetchone()

    return int(result[0]) if result else 0


def get_min_max_id(table_name):
    from jetshift_core.js_logger import get_logger
    logger = get_logger(__name__)

    try:
        # Use context management for connection handling (assuming clickhouse_client supports it)
        with clickhouse_client() as clickhouse:
            # Execute query and get the result
            result = clickhouse.execute(f"SELECT MIN(id), MAX(id) FROM {table_name}")

            # Return min and max if result is valid
            return result[0] if result else (-1, -1)
    except Exception as e:
        logger.error("Failed to get min and max ID for table '%s': %s", table_name, e)
        return -1, -1


def insert_into_clickhouse(target_engine, table_name, data_chunk):
    import pandas as pd
    from sqlalchemy import MetaData, Table, insert
    from jetshift_core.js_logger import get_logger
    js_logger = get_logger()

    last_inserted_id = None

    if not data_chunk.empty:
        try:
            # Reflect the ClickHouse table
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=target_engine)

            # Convert DataFrame chunk to list of dicts for SQLAlchemy insert
            records = data_chunk.where(pd.notnull(data_chunk), None).to_dict(orient='records')

            with target_engine.connect() as connection:
                connection.execute(insert(table), records)

            # If 'id' column exists, fetch the last inserted 'id' from the chunk
            if 'id' in data_chunk.columns:
                last_inserted_id = data_chunk['id'].iloc[-1]

            return True, last_inserted_id

        except Exception as e:
            js_logger.error(f"{table_name}: Error inserting into ClickHouse. Error: {str(e)}")
            return False, last_inserted_id
    else:
        js_logger.info(f'{table_name}: No data to insert')
        return False, last_inserted_id


def insert_update_clickhouse(table_name, table_fields, data):
    from jetshift_core.helpers.common import send_discord_message

    if data:
        currencies = [row[1] for row in data]  # Assuming 'currency' is the second column
        updates = [f"WHEN currency = '{row[1]}' THEN {row[2]}" for row in data]  # 'rate' is the third column

        clickhouse = clickhouse_client()

        # Generate the bulk update query
        clickhouse_query = f"""
            ALTER TABLE {table_name} 
            UPDATE rate = CASE 
            {' '.join(updates)} 
            END,
            updated_at = now()
            WHERE currency IN ({', '.join(f"'{currency}'" for currency in currencies)});
            """

        clickhouse.execute(clickhouse_query)
        clickhouse.disconnect_connection()

        # Count the number of rows inserted
        num_rows_inserted = len(data)

        send_discord_message(f'{table_name}: Updated {num_rows_inserted} rows')

        return True
    else:
        send_discord_message(f'{table_name}: No data to insert')
        return False


def truncate_table(target_engine, table_name):
    from sqlalchemy import text
    from jetshift_core.js_logger import get_logger
    logger = get_logger(__name__)

    try:
        with target_engine.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE {table_name}"))
        return True
    except Exception as e:
        logger.error(f'{table_name}: Failed to truncate table. Error: {str(e)}')
        return False
