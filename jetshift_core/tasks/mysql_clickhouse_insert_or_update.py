from luigi.format import UTF8
from jetshift_core.helpers.common import *
from jetshift_core.helpers.mysql import *
from jetshift_core.helpers.clcikhouse import insert_update_clickhouse
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
            fetch_and_extract(engine, self.table_name, self.output()['extracted'].path, table_fields)

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

        # Load CSV in chunks
        for chunk in pd.read_csv(input_file, chunksize=self.chunk_size):
            data = format_csv_data(chunk, fields)  # Assuming format_csv_data can handle DataFrame input

            # Insert data into ClickHouse
            success = insert_update_clickhouse(self.table_name, table_fields, data)
            if not success:
                print("Failed to insert data into ClickHouse.")
                break

    def run(self):
        # Step 1: Extract data from RDS
        self.extract()

        # Step 2: Transform the extracted data
        # self.transform()

        # Step 3: Load transformed data to ClickHouse
        self.load()


def fetch_and_extract(engine, table_name, output_path, table_fields):
    query = f"SELECT {', '.join(table_fields)} FROM {table_name}"
    df = pd.read_sql(query, engine)

    create_data_directory()
    df.to_csv(output_path, index=False)
