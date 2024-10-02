from faker import Faker
import datetime

fake = Faker()


def generate_fake_data(table, fields):
    fake = Faker()
    formatted_row = []

    for field_name, field_type in fields:
        column = table.columns[field_name]
        field_length = column.type.length if hasattr(column.type, 'length') and field_type == str else None

        if field_type == int and field_name != 'id':
            value = fake.random_int()
        elif field_type == str and field_name == 'name':
            value = fake.name()
        elif field_type == str and 'mail' in field_name:
            value = fake.email()
        elif field_type == str:
            value = fake.sentence(nb_words=6, variable_nb_words=True) if field_length is None else fake.text(max_nb_chars=field_length)
        elif field_type == float:
            value = fake.random_number(digits=5, fix_len=True) / 100.0
        elif field_type == datetime.datetime:
            value = fake.date_time_this_decade()
        else:
            value = None

        if value is not None:
            formatted_row.append(value)

    return tuple(formatted_row)
