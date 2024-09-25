from config.luigi import luigi, local_scheduler
from jetshift_core.tasks.mysql_clickhouse_insert import BaseTask


class job_class_name(BaseTask):
    table_name = 'the_table_name'


def main():
    luigi.build([job_class_name(
        live_schema=True,
        extract_limit=10,
        extract_chunk_size=5,
        load_chunk_size=5
    )], local_scheduler=local_scheduler)


if __name__ == '__main__':
    main()
