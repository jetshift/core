from clickhouse_driver import Client
from clickhouse_driver.errors import Error


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
    from config.logging import logger

    try:
        with clickhouse_client() as clickhouse:
            result = clickhouse.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return len(result) > 0
    except Exception as e:
        logger.error("Failed to check data for table '%s': %s", table_name, e)
        return False


def get_last_id_from_clickhouse(table_name, primary_id='id'):
    clickhouse = clickhouse_client()
    data = clickhouse.execute(f"SELECT {primary_id} FROM {table_name} ORDER BY {primary_id} DESC LIMIT 1")
    clickhouse.disconnect_connection()
    return int(data[0][0]) if data else 0


def get_min_max_id(table_name):
    from config.logging import logger

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


def insert_into_clickhouse(table_name, table_fields, data):
    from jetshift_core.helpers.common import send_discord_message
    from config.logging import logger

    last_inserted_id = None

    # print()
    # print(data)
    # print()

    if data:
        try:
            clickhouse = clickhouse_client()
            clickhouse_query = f"INSERT INTO {table_name} ({', '.join(table_fields)}) VALUES"
            clickhouse.execute(clickhouse_query, data)
            clickhouse.disconnect_connection()

            # Check if 'id' is in the table fields and get the last inserted id
            if 'id' in table_fields:
                last_inserted_id = data[-1][table_fields.index('id')]

                return True, last_inserted_id
            else:
                return True, last_inserted_id
        except Error as e:
            logger.error(f"{table_name}: Error inserting into ClickHouse. Error: {str(e)}")
            return False, last_inserted_id
    else:
        send_discord_message(f'{table_name}: No data to insert')
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


def truncate_table(table_name):
    from config.logging import logger

    try:
        clickhouse = clickhouse_client()

        # Generate the bulk update query
        clickhouse_query = f"""TRUNCATE TABLE {table_name}"""

        clickhouse.execute(clickhouse_query)
        clickhouse.disconnect_connection()
        return True
    except Error as e:
        logger.error(f'{table_name}: Failed to truncate table. Error: {str(e)}')
        return False
