import yaml
from jetshift_core.utils.database.sqlalchemy_mysql import *
from sqlalchemy.dialects.mysql import *
from sqlalchemy.sql import *

# Map YAML types to SQLAlchemy types
type_mapping = {
    'INT': INTEGER,
    'VARCHAR': String,
    'TIMESTAMP': DateTime,
    'DECIMAL': DECIMAL,
    'BOOLEAN': BOOLEAN,
    'FLOAT': FLOAT,
    'DATE': DATE,
}


def parse_column_type(col_type_str):
    if '(' in col_type_str and ')' in col_type_str:
        base_type, params = col_type_str.split('(')
        params = params.split(')')[0].split(',')
        if len(params) == 1:
            length = int(params[0])
            return type_mapping.get(base_type, base_type)(length)
        elif len(params) == 2:
            precision, scale = map(int, params)

            return type_mapping.get(base_type, base_type)(precision, scale)
    return type_mapping.get(col_type_str, col_type_str)()


def yaml_table_definition(file_path):
    with open(file_path, 'r') as file:
        schema = yaml.safe_load(file)

    # Extract table name and columns
    table_name = schema['table_name']
    columns = schema['columns']
    dependencies = schema.get('dependencies', '')
    data = schema.get('data', False) if isinstance(schema.get('data', False), bool) else False

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
        elif 'default' in column:
            col_args['server_default'] = column['default']
        elif 'on_update' in column and column['on_update'] == 'CURRENT_TIMESTAMP':
            col_args['onupdate'] = func.now()

        custom_column_info = {
            'seeder': column.get('seeder', None)
        }

        sqlalchemy_columns.append(Column(column['name'], col_type, info=custom_column_info, **col_args))

    custom_table_info = {
        'dependencies': [dep.strip() for dep in dependencies.split(',')] if dependencies else [],
        'data': data
    }

    # Define the table
    return Table(
        table_name,
        metadata,
        *sqlalchemy_columns,
        extend_existing=True,
        info=custom_table_info,
    )


def migrate(file_path, fresh, drop):
    table_def = yaml_table_definition(file_path)

    # Create the table
    create_table(table_def, fresh, drop)
