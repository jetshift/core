from config.luigi import luigi
from jetshift_core.tasks.mysql_clickhouse_insert_or_update import BaseTask


class job_class_name(BaseTask):
    table_name = 'the_table_name'


def main():
    luigi.build([job_class_name(limit=10, chunk_size=5)], local_scheduler=True)


if __name__ == '__main__':
    main()
