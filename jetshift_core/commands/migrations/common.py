import decimal
import random
import datetime
from faker import Faker
from jetshift_core.commands.seeders.common import min_max_id

fake = Faker()


def get_faker_value(command):
    try:
        result = eval(command, {"fake": fake})
        return result
    except Exception as e:
        print(f"Error: {e}")
        return None


def get_random_value(command):
    try:
        result = eval(command, {"random": random})
        return result
    except Exception as e:
        print(f"Error: {e}")
        return None


def generate_data_from_seeder_info(engine, column, field_length, seeder_info):
    value = None

    if 'fake.' in seeder_info:
        value = get_faker_value(seeder_info)
    elif 'random.' in seeder_info:
        value = get_random_value(seeder_info)
    else:
        # seeder_function
        if '(' in seeder_info and ')' in seeder_info:
            seeder_function, seeder_param = seeder_info.split('(')
            seeder_param = seeder_param.rstrip(')')
        else:
            seeder_function, seeder_param = seeder_info, ''

        if seeder_function == 'min_max':
            value = random.randint(*min_max_id(engine, seeder_param))

    return value


def generate_fake_data(engine, table, fields):
    fake = Faker()
    formatted_row = []

    for field_name, field_type in fields:
        column = table.columns[field_name]
        field_length = column.type.length if hasattr(column.type, 'length') and field_type == str else None

        # if hasattr(column.type, 'length'):
        #     print(f"VARCHAR length: {column.type.length}")
        # elif hasattr(column.type, 'precision') and hasattr(column.type, 'scale'):
        #     print(f"DECIMAL precision: {column.type.precision}, scale: {column.type.scale}")
        # else:
        #     print(column.type)

        # print(column.type)

        seeder_info = column.info.get('seeder', None)
        if seeder_info is not None:
            value = generate_data_from_seeder_info(engine, column, field_length, seeder_info)

        elif field_type == int and field_name != 'id':
            if field_length is not None:
                value = (fake.random_int(0, field_length))
            else:
                value = fake.random_int()

        elif field_type == str and field_name == 'name':
            value = fake.name()

        elif field_type == bool:
            value = random.choice([0, 1])

        elif field_type == str:
            # Generate text with a length of 1-4 characters
            if field_length is not None and field_length <= 4:
                pattern = '?' * field_length
                sentences = fake.lexify(text=pattern)
            else:
                sentences = fake.sentence(nb_words=2, variable_nb_words=True) if field_length is None else fake.text(max_nb_chars=field_length)
            value = sentences

        elif field_type == float:
            value = fake.random_number(digits=5, fix_len=True) / 100.0

        elif field_type == decimal.Decimal:
            value = decimal.Decimal(fake.random_number(digits=5, fix_len=True) / 100.0)

        elif field_type == datetime.datetime:
            value = fake.date_time_this_decade()
        else:
            value = None

        if value is not None:
            formatted_row.append(value.strip() if isinstance(value, str) else value)

    return tuple(formatted_row)
