import sys


def migrations(items):
    from core.runners.migration import main as run_migration
    for item in items:
        sys.argv = [__file__, item, None, 'f']
        run_migration()


def seeders(items):
    from core.runners.seeder import main as run_seeder
    for item in items:
        params_split = item.split(' -n ')
        seeder_name = params_split[0]
        if len(params_split) > 1:
            params = params_split[1]
        else:
            params = None

        if not params:
            print(f"Seeder '{seeder_name}' requires parameters.")
            sys.exit(1)

        sys.argv = [__file__, 'mysql', seeder_name, '-n', params]
        run_seeder()


def job(items):
    from core.runners.job import main as run_job
    for item in items:
        sys.argv = [__file__, item]
        run_job()


def main():
    migrations_list = ["mysql", "clickhouse"]
    migrations(migrations_list)

    migrations_list = ["migration1 -n 5", "migration2 -n 10"]
    seeders(migrations_list)

    job_list = ["job1", "job2"]
    job(job_list)


if __name__ == "__main__":
    main()
