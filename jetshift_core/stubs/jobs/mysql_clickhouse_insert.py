from config.luigi import luigi, local_scheduler
from jetshift_core.tasks.mysql_clickhouse_insert import BaseTask


class job_class_name(BaseTask):
    table_name = 'the_table_name'


def main():
    luigi.build([job_class_name(
        live_schema=False,
        primary_id='id',
        # extract
        extract_offset=0,
        extract_limit=10,
        # extract_chunk_size=20,
        # load
        truncate_table=False,
        load_chunk_size=10,
        sleep_interval=1
    )], local_scheduler=local_scheduler)


if __name__ == '__main__':
    main()
