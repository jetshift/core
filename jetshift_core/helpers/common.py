import os
import requests


def create_data_directory():
    data_folder_path = os.path.abspath('data')
    if not os.path.exists(data_folder_path):
        os.makedirs(data_folder_path)


def to_pascal_case(job_name):
    # Split the job name by underscores or spaces, capitalize each word, and join them together
    return ''.join(word.capitalize() for word in job_name.split('_'))


def format_csv_data(df, fields):
    import pandas as pd
    formatted_data = []
    for _, row in df.iterrows():
        formatted_row = []
        for field_name, field_type in fields:
            value = row[field_name]

            # print(f"field_name: {field_name}")
            # print(f"field_type: {field_type}")
            # print(f"value: {value}")
            # print()

            if pd.isna(value):
                # Set the formatted value to None to represent NULL in ClickHouse
                formatted_value = None
            else:
                # Convert value to float if field_name is price or rrp
                if field_name in ['price', 'rrp', 'cost']:
                    formatted_value = float(value)
                else:
                    # Convert value to the desired type if a conversion function is provided
                    formatted_value = field_type(value) if callable(field_type) else value

            formatted_row.append(formatted_value)

        # print(formatted_row)
        # print()

        formatted_data.append(tuple(formatted_row))
    return formatted_data


def ClearFiles(table_name):
    paths = [
        f"data/{table_name}.csv",
        f"data/transformed_{table_name}.csv"
    ]

    print(f"Clearing files: {paths}")
    for path in paths:
        if os.path.exists(path):
            os.remove(path)
            print(f"Deleted: {path}")

    print()


def send_discord_message(message):
    import json
    from dotenv import load_dotenv
    load_dotenv()
    
    if len(message) == 0:
        return None

    secrets_json = os.environ.get('SECRETS_JSON')
    if secrets_json:
        secrets = json.loads(secrets_json)
        webhook_url = secrets.get('webhook_url')
    else:
        webhook_url = os.environ.get('DISCORD_WEBHOOK')

    truncated_message = message[:500]

    data = {
        "content": truncated_message,
        "username": "EF ETL Bot"
    }
    response = requests.post(webhook_url, json=data)

    if response.status_code != 204:
        print(f"Failed to send message: {response.status_code}, {response.text}")

    return None


def run_job_in_new_process(module_name):
    import importlib
    import multiprocessing
    from config.logging import logger

    try:
        job_module = importlib.import_module(module_name)
        if hasattr(job_module, 'main'):
            p = multiprocessing.Process(target=job_module.main)
            p.start()
            p.join()
            return True
    except ImportError as e:
        logger.error(f"Error importing module {module_name}: {e}")
    except Exception as e:
        logger.error(f"Error running main function in module {module_name}: {e}")
    return False


# Reflect database structure from file
def get_mysql_table_fields(table_name):
    import importlib

    module_path = f"database.migrations.mysql.{table_name}"

    table_module = importlib.import_module(module_path)

    table = getattr(table_module, 'table')

    fields = [(col.name, col.type.python_type) for col in table.columns]

    return fields


# Reflect the existing database structure
def get_mysql_table_fields_from_database(table_name):
    from jetshift_core.utils.database.sqlalchemy_mysql import get_engine, MetaData, Table

    engine = get_engine()
    metadata = MetaData()
    metadata.reflect(bind=engine)

    # Access the users table using the reflected metadata
    table = Table(table_name, metadata, autoload_with=engine)

    # Extract column information
    fields = [(col.name, col.type.python_type) for col in table.columns]

    return fields


def convert_field_to_python(field_type):
    """Converts SQLAlchemy column types to Python types for the field dictionary."""
    from datetime import datetime

    type_mappings = {
        int: int,
        str: str,
        datetime: lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
    }
    return type_mappings.get(field_type, str)
