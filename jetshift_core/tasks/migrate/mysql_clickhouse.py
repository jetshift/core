import pandas as pd
import time
from prefect import flow, task, get_run_logger
from jetshift_core.helpers.clcikhouse import insert_into_clickhouse
from app.services.database import get_migrate_table_by_id, create_database_engine
from jetshift_core.helpers.common import *
from jetshift_core.helpers.mysql import *


# Test
# from jetshift_core.js_logger import get_logger
# logger = get_logger(__name__)


@task
def extract_data(params):
    flogger = get_run_logger()

    try:
        if params.extract_limit:
            fetch_and_extract_limit(params)
        else:
            fetch_and_extract_chunk(params)
        flogger.info(f"Data extracted to {params.output_path}")
    except Exception as e:
        flogger.error(f"Extraction failed: {str(e)}")
        raise e
    return params.output_path


@task
def load_data(params):
    flogger = get_run_logger()

    if not os.path.exists(params.output_path):
        flogger.warning(f"No data to load for {params.target_table}")
        return False

    num_rows = 0
    last_inserted_id = None
    date_columns = ['created_at', 'updated_at', 'email_verified_at']  # Adjust as needed

    try:
        for chunk in pd.read_csv(params.output_path, chunksize=params.load_chunk_size, parse_dates=date_columns):
            success, last_inserted_id = insert_into_clickhouse(params.target_engine, params.target_table, chunk)
            if success:
                num_rows += len(chunk)
                flogger.info(f"Inserted {len(chunk)} rows. Last ID {last_inserted_id}")
            time.sleep(params.sleep_interval)

        flogger.info(f"Total inserted: {num_rows} rows into {params.target_table}")
    except Exception as e:
        flogger.error(f"Load failed: {str(e)}")
        raise e

    return True


@flow(name="MySQL to ClickHouse Migration")
def mysql_to_clickhouse_flow(migrate_table_obj, task):
    from app.services.migrate.common import AttrDict
    print('---------------')

    params = AttrDict(dict(
        source_engine=create_database_engine(migrate_table_obj.source_db),
        target_engine=create_database_engine(migrate_table_obj.target_db),
        source_table=task.source_table,
        target_table=task.target_table,
        live_schema=False,
        primary_id='id',
        extract_offset=0,
        extract_limit=10,
        extract_chunk_size=50,
        truncate_table=False,
        load_chunk_size=10,
        sleep_interval=1
    ))

    params.output_path = f"data/{params.source_table}.csv"

    extract_data(params)
    load_data(params)
