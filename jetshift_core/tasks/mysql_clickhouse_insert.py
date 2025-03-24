import luigi
import pandas as pd
import time
from jetshift_core.js_logger import get_logger
from luigi.format import UTF8
from jetshift_core.helpers.common import *
from jetshift_core.helpers.mysql import *
from jetshift_core.helpers.clcikhouse import insert_into_clickhouse

logger = get_logger(__name__)


class MysqlToClickhouse(luigi.Task):
    source_engine = luigi.Parameter()
    target_engine = luigi.Parameter()
    source_table = luigi.Parameter()
    target_table = luigi.Parameter()

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
            'extracted': luigi.LocalTarget(f'data/{self.source_table}.csv', format=UTF8),
            'transformed': luigi.LocalTarget(f'data/transformed_{self.source_table}.csv')
        }

    def extract(self):
        try:
            if self.extract_limit != 0:
                fetch_and_extract_limit(self, self.source_engine, self.target_engine)
            else:
                fetch_and_extract_chunk(self, self.source_engine, self.target_engine)

        except Exception as e:
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

            # check input_file has exists
            if not os.path.exists(input_file):
                print(f'No data to load for {self.source_table}')
                return False

            num_rows = 0
            last_inserted_id = None
            date_columns = ['created_at', 'updated_at', 'email_verified_at']  # Replace with your actual datetime columns

            # Load CSV in chunks
            print(f'\nLoading data into ClickHouse...')
            for chunk in pd.read_csv(input_file, chunksize=self.load_chunk_size, parse_dates=date_columns):
                # Insert data into ClickHouse
                success, last_inserted_id = insert_into_clickhouse(self.target_engine, self.target_table, chunk)
                if success:
                    num_rows += len(chunk)
                    print(f'Inserted {len(chunk)} rows into {self.target_table}. Last ID {last_inserted_id}')

                # Sleep for a specified interval to manage load
                time.sleep(self.sleep_interval)

            # Send Discord message
            if num_rows > 0:
                if last_inserted_id is not None:
                    # send_discord_message(f'{self.target_table}: Inserted {num_rows} rows. Last id {last_inserted_id}')
                    logger.info(f'{self.target_table}: Inserted {num_rows} rows. Last id {last_inserted_id}')
                else:
                    # send_discord_message(f'{self.target_table}: Inserted {num_rows} rows')
                    logger.info(f'{self.target_table}: Inserted {num_rows} rows')

                logger.info(f'---------\nTotal inserted {num_rows} rows into {self.target_table}. Last ID {last_inserted_id}')
        except Exception as e:
            logger.error(e)


def run(self):
    # Step 1: Extract data from RDS
    self.extract()

    # Step 2: Transform the extracted data
    # self.transform()

    # Step 3: Load transformed data to ClickHouse
    self.load()
