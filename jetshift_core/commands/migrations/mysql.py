import yaml
from jetshift_core.utils.database.sqlalchemy_mysql import *
from sqlalchemy.dialects.mysql import *
from sqlalchemy.sql import *

# Map YAML types to SQLAlchemy types
type_mapping = {
    'INT': INTEGER,
    'VARCHAR': String,
    'TIMESTAMP': DateTime,
}


def parse_column_type(col_type_str):
    if '(' in col_type_str and ')' in col_type_str:
        base_type, length = col_type_str.split('(')
        length = int(length.split(')')[0])
        return type_mapping.get(base_type, base_type)(length)
    return type_mapping.get(col_type_str, col_type_str)()


def migrate(file_path, fresh):
    with open(file_path, 'r') as file:
        schema = yaml.safe_load(file)

    # Extract table name and columns
    table_name = schema['table_name']
    columns = schema['columns']

    # Define columns for SQLAlchemy table
    sqlalchemy_columns = []
    for column in columns:
        col_type = parse_column_type(column['type'])
        col_args = {
            'primary_key': column.get('primary_key', False),
            'autoincrement': column.get('auto_increment', False),
            'nullable': column.get('nullable', True),
        }
        if 'default' in column and column['default'] == 'CURRENT_TIMESTAMP':
            col_args['server_default'] = func.now()
        if 'on_update' in column and column['on_update'] == 'CURRENT_TIMESTAMP':
            col_args['onupdate'] = func.now()
        sqlalchemy_columns.append(Column(column['name'], col_type, **col_args))

    # Define the table
    table = Table(table_name, metadata, *sqlalchemy_columns)

    # Create the table
    create_table(table, fresh)
