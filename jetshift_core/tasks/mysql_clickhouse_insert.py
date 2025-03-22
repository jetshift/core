import luigi
import pandas as pd
import time
from jetshift_core.js_logger import get_logger
from luigi.format import UTF8
from jetshift_core.helpers.common import *
from jetshift_core.helpers.mysql import *
from jetshift_core.helpers.clcikhouse import insert_into_clickhouse, get_last_id_from_clickhouse

from app.services.database import get_migrate_table_by_id, create_database_engine

logger = get_logger(__name__)


class BaseTask(luigi.Task):
    table_name = luigi.Parameter()

    migrate_table_id = luigi.IntParameter(default=None)

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

    def extract(self):
        try:
            migrate_table_obj = get_migrate_table_by_id(self.migrate_table_id)
            source_engine = create_database_engine(migrate_table_obj.source_db)
            target_engine = create_database_engine(migrate_table_obj.target_db)

            if self.extract_limit != 0:
                fetch_and_extract_limit(self, source_engine, target_engine)
            else:
                fetch_and_extract_chunk(self, source_engine, target_engine)

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

            migrate_table_obj = get_migrate_table_by_id(self.migrate_table_id)
            target_engine = create_database_engine(migrate_table_obj.target_db)

            # check input_file has exists
            if not os.path.exists(input_file):
                print(f'No data to load for {self.table_name}')
                return False

            num_rows = 0
            last_inserted_id = None
            date_columns = ['created_at', 'updated_at', 'email_verified_at']  # Replace with your actual datetime columns

            # Load CSV in chunks
            print(f'\nLoading data into ClickHouse...')
            for chunk in pd.read_csv(input_file, chunksize=self.load_chunk_size, parse_dates=date_columns):
                # Insert data into ClickHouse
                success, last_inserted_id = insert_into_clickhouse(target_engine, self.table_name, chunk)
                if success:
                    num_rows += len(chunk)
                    print(f'Inserted {len(chunk)} rows into {self.table_name}. Last ID {last_inserted_id}')

                # Sleep for a specified interval to manage load
                time.sleep(self.sleep_interval)

            # Send Discord message
            if num_rows > 0:
                if last_inserted_id is not None:
                    # send_discord_message(f'{self.table_name}: Inserted {num_rows} rows. Last id {last_inserted_id}')
                    logger.info(f'{self.table_name}: Inserted {num_rows} rows. Last id {last_inserted_id}')
                else:
                    # send_discord_message(f'{self.table_name}: Inserted {num_rows} rows')
                    logger.info(f'{self.table_name}: Inserted {num_rows} rows')

                logger.info(f'---------\nTotal inserted {num_rows} rows into {self.table_name}. Last ID {last_inserted_id}')
        except Exception as e:
            logger.error(e)

    def run(self):
        # Step 1: Extract data from RDS
        self.extract()

        # Step 2: Transform the extracted data
        # self.transform()

        # Step 3: Load transformed data to ClickHouse
        self.load()
