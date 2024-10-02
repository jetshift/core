import yaml
from jetshift_core.utils.database.sqlalchemy_clickhouse import *
from clickhouse_sqlalchemy import types, engines

# Map YAML types to SQLAlchemy types
type_mapping = {
    'INT': types.UInt32,
    'VARCHAR': types.String,
    'TIMESTAMP': types.DateTime,
}


def parse_column_type(col_type_str, nullable):
    if '(' in col_type_str and ')' in col_type_str:
        base_type, length = col_type_str.split('(')
        length = int(length.split(')')[0])
        col_type = type_mapping.get(base_type, base_type)(length)
    else:
        col_type = type_mapping.get(col_type_str, col_type_str)()

    if nullable:
        col_type = types.Nullable(col_type)

    return col_type


def yaml_table_definition(file_path):
    with open(file_path, 'r') as file:
        schema = yaml.safe_load(file)

    # Extract table name and columns
    table_name = schema['table_name']
    columns = schema['columns']

    # Define columns for SQLAlchemy table
    sqlalchemy_columns = []
    for column in columns:
        col_type = parse_column_type(column['type'], column.get('nullable', False))
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
    return Table(
        table_name,
        metadata,
        *sqlalchemy_columns,
        engines.MergeTree(order_by=['id'])
    )


def migrate(file_path, fresh):
    table_def = yaml_table_definition(file_path)

    # Create the table
    create_table(table_def, fresh)
