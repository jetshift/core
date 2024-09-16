import sys


def migrations(items):
    from jetshift_core.runners.migration import main as run_migration
    for item in items:
        sys.argv = [__file__, item, None, 'f']
        run_migration()


def seeders(items):
    from jetshift_core.runners.seeder import main as run_seeder
    for item in items:
        params_split = item.split(' -n ')
        seeder_name = params_split[0]
        if len(params_split) > 1:
            params = params_split[1]
            sys.argv = [__file__, 'mysql', seeder_name, '-n', params]
        else:
            sys.argv = [__file__, 'mysql', seeder_name]

        run_seeder()


def jobs(items):
    from jetshift_core.runners.job import main as run_job
    for item in items:
        sys.argv = [__file__, item]
        run_job()
