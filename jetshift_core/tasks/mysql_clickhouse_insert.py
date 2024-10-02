import luigi
import pandas as pd
import time
from config.logging import logger
from luigi.format import UTF8
from jetshift_core.helpers.common import *
from jetshift_core.helpers.mysql import *
from jetshift_core.helpers.clcikhouse import insert_into_clickhouse, get_last_id_from_clickhouse


class BaseTask(luigi.Task):
    table_name = luigi.Parameter()
    live_schema = luigi.BoolParameter(default=False)
    primary_id = luigi.Parameter(default='')

    extract_offset = luigi.IntParameter(default=0)
    extract_limit = luigi.IntParameter(default=0)
    extract_chunk_size = luigi.IntParameter(default=100)

    truncate_table = luigi.BoolParameter(default=False)
    load_chunk_size = luigi.IntParameter(default=100)
    sleep_interval = luigi.FloatParameter(default=1)

    def output(self):
        return {
            'extracted': luigi.LocalTarget(f'data/{self.table_name}.csv', format=UTF8),
            'transformed': luigi.LocalTarget(f'data/transformed_{self.table_name}.csv')
        }

    def get_mysql_fields(self):
        table = get_mysql_table_definition(self.table_name, self.live_schema)
        columns = [(col.name, col.type.python_type) for col in table.columns]

        fields = [(field[0], convert_field_to_python(field[1])) for field in columns]
        table_fields = [field[0] for field in fields]

        return fields, table_fields

    def extract(self):
        try:
            engine = mysql_client()
            # Handle connection failure
            if isinstance(engine, dict):
                return

            fields, table_fields = self.get_mysql_fields()

            if self.extract_limit != 0:
                fetch_and_extract_limit(self, engine, table_fields)
            else:
                fetch_and_extract_chunk(self, engine, table_fields)

        except Exception as e:
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
        try:
            input_file = self.output()['extracted'].path
            fields, table_fields = self.get_mysql_fields()

            # check input_file has exists
            if not os.path.exists(input_file):
                print(f'No data to load for {self.table_name}')
                return False

            num_rows = 0
            last_inserted_id = None

            # print()
            # print(fields)
            # print()

            # Load CSV in chunks
            print(f'\nLoading data into ClickHouse...')
            for chunk in pd.read_csv(input_file, chunksize=self.load_chunk_size):
                data = format_csv_data(chunk, fields)  # Assuming format_csv_data can handle DataFrame input

                # Insert data into ClickHouse
                success, last_inserted_id = insert_into_clickhouse(self.table_name, table_fields, data)
                if success:
                    num_rows += len(data)
                    print(f'Inserted {len(data)} rows into {self.table_name}. Last ID {last_inserted_id}')

                # Sleep for a specified interval to manage load
                time.sleep(self.sleep_interval)

            # Send Discord message
            if num_rows > 0:
                if last_inserted_id is not None:
                    send_discord_message(f'{self.table_name}: Inserted {num_rows} rows. Last id {last_inserted_id}')
                else:
                    send_discord_message(f'{self.table_name}: Inserted {num_rows} rows')

                print(f'---------\nTotal inserted {num_rows} rows into {self.table_name}. Last ID {last_inserted_id}')
        except Exception as e:
            print(e)
            logger.error(e)

    def run(self):
        print('Running job for table: ', self.table_name)

        # Step 1: Extract data from RDS
        self.extract()

        # Step 2: Transform the extracted data
        # self.transform()

        # Step 3: Load transformed data to ClickHouse
        self.load()
