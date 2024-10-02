import os
import requests
import importlib
import multiprocessing
from config.logging import logger


def jprint(message, type='info', all=False, key=None):
    import click

    # Map message types to corresponding labels and colors
    type_map = {
        'info': (f"{key or 'INFO '} ", 'blue'),
        'success': (f"{key or 'SUCCESS '} ", 'green'),
        'error': (f"{key or 'ERROR '} ", 'red')
    }

    label, color = type_map.get(type, ('INFO ', 'blue'))  # Default to 'info' type

    if all:
        click.echo(click.style(message + '\n', fg=color), err=(type == 'error'))
    else:
        click.echo(click.style(label, fg=color) + message + '\n', err=(type == 'error'))


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

            if field_type == int and pd.isna(value):
                formatted_value = 0
            elif pd.isna(value):
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


def clear_files(table_name):
    paths = [
        f"data/{table_name}.csv",
        f"data/transformed_{table_name}.csv"
    ]

    for path in paths:
        if os.path.exists(path):
            os.remove(path)


def send_discord_message(message):
    import json
    from dotenv import load_dotenv
    load_dotenv()

    if len(message) == 0:
        return None

    secrets_json = os.environ.get('SECRETS_JSON')
    if secrets_json:
        secrets = json.loads(secrets_json)
        webhook_url = secrets.get('DISCORD_WEBHOOK')
    else:
        webhook_url = os.environ.get('DISCORD_WEBHOOK')

    if not webhook_url:
        return None

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


def run_command_subprocess(command):
    import click
    import subprocess
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, text=True, universal_newlines=True)

        # Read stdout line by line as it becomes available
        for line in process.stdout:
            click.echo(line, nl=False)

        process.stdout.close()
        process.wait()
    except Exception as e:
        logger.error(f"Error running command {command}: {e}")
    return False


def run_multi_process(function_to_call, *params):
    logger.info(f"Running function: {function_to_call.__name__} with params: {params}")

    try:
        p = multiprocessing.Process(target=function_to_call, args=params)
        p.start()
        logger.info("Process started successfully.")
    except Exception as e:
        logger.error(f"An error occurred while running the process: {str(e)}")


def convert_field_to_python(field_type):
    """Converts SQLAlchemy column types to Python types for the field dictionary."""
    from datetime import datetime

    type_mappings = {
        int: int,
        str: str,
        float: float,
        datetime: lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
    }
    return type_mappings.get(field_type, str)
