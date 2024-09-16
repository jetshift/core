from luigi.format import UTF8
from jetshift_core.helpers.common import *
from jetshift_core.helpers.mysql import *
from jetshift_core.helpers.clcikhouse import insert_into_clickhouse, truncate_table
import luigi
import pandas as pd
import pymysql
from config.logging import logger


class BaseTask(luigi.Task):
    table_name = luigi.Parameter()
    live_schema = luigi.BoolParameter(default=False)
    limit = luigi.IntParameter(default=20)
    chunk_size = luigi.IntParameter(default=5)

    def output(self):
        return {
            'extracted': luigi.LocalTarget(f'data/{self.table_name}.csv', format=UTF8),
            'transformed': luigi.LocalTarget(f'data/transformed_{self.table_name}.csv')
        }

    def get_fields(self):
        if self.live_schema:
            fields_data = get_mysql_table_fields_from_database(self.table_name)
        else:
            fields_data = get_mysql_table_fields(self.table_name)

        fields = [(field[0], convert_field_to_python(field[1])) for field in fields_data]
        table_fields = [field[0] for field in fields]
        return fields, table_fields

    def extract(self):
        try:
            engine = mysql_client()
            # Handle connection failure
            if isinstance(engine, dict):
                return

            fields, table_fields = self.get_fields()
            fetch_and_extract(engine, self.table_name, self.output()['extracted'].path, table_fields, self.limit)

        except pymysql.MySQLError as e:
            print(e)
            logger.error(e)

    def transform(self):
        input_file = self.output()['extracted']
        with input_file.open('r') as infile, self.output()['transformed'].open('w') as outfile:
            for line in infile:
                columns = line.strip().split(',')
                print('Transforming: ', columns)

                transformed_line = ','.join(columns)
                outfile.write(transformed_line + '\n')

    def load(self):
        input_file = self.output()['extracted'].path
        fields, table_fields = self.get_fields()

        num_rows = 0
        last_inserted_id = None

        # Load CSV in chunks
        for chunk in pd.read_csv(input_file, chunksize=self.chunk_size):
            data = format_csv_data(chunk, fields)  # Assuming format_csv_data can handle DataFrame input

            # Insert data into ClickHouse
            success, last_inserted_id = insert_into_clickhouse(self.table_name, table_fields, data)
            if success:
                num_rows += len(data)

        # Send Discord message
        if num_rows > 0:
            if last_inserted_id is not None:
                send_discord_message(f'{self.table_name}: Inserted {num_rows} rows. Last inserted id {last_inserted_id}')
            else:
                send_discord_message(f'{self.table_name}: Inserted {num_rows} rows')

            print(f'{self.table_name}: Inserted {num_rows} rows. Last inserted id {last_inserted_id}')

    def run(self):
        # Step 1: Extract data from RDS
        self.extract()

        # Step 2: Transform the extracted data
        # self.transform()

        # Step 3: Load transformed data to ClickHouse
        self.load()


def fetch_and_extract(engine, table_name, output_path, table_fields, limit):
    create_data_directory()
    truncate_table(table_name)

    # Count total rows
    count_query = f"SELECT COUNT(*) FROM {table_name}"
    total_rows = pd.read_sql(count_query, engine).iloc[0, 0]
    print(f"Total rows in {table_name}: {total_rows}")

    loops = total_rows // limit + (1 if total_rows % limit != 0 else 0)
    print(f"Total loops: {loops}")

    for i in range(loops):
        offset = i * limit
        query = f"SELECT {', '.join(table_fields)} FROM {table_name} LIMIT {limit} OFFSET {offset}"
        df = pd.read_sql(query, engine)
        if i == 0:
            df.to_csv(output_path, index=False)
        else:
            df.to_csv(output_path, mode='a', header=False, index=False)
