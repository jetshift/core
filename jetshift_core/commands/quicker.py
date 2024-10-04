import glob
import os
import sys
import yaml
import click
from jetshift_core.helpers.quicker import run_migrations, run_seeders, run_jobs
from jetshift_core.helpers.common import jprint


def prepare_migration(config):
    migration_config = config.get('migrations', {})
    engines = migration_config.get('engines', ['mysql'])
    names = migration_config.get('names', [])
    fresh = migration_config.get('fresh', True)

    return engines, names, fresh


def prepare_seeders(config):
    seeder_config = config.get('seeders', [])
    engines = seeder_config.get('engines', ['mysql'])
    names = seeder_config.get('names', [])

    if any('all' in name for name in names):
        params = next(name for name in names if 'all' in name).split()[1:]
        names = [os.path.splitext(os.path.basename(file))[0] for file in glob.glob('app/migrations/*.yaml')]
        names = [f"{name} {' '.join(params)}" for name in names]

    return engines, names


def prepare_jobs(config, seeder_list):
    job_list = config.get('jobs', [])

    if 'all' in job_list:
        job_list = [os.path.splitext(os.path.basename(file))[0] for file in glob.glob('app/jobs/*.yaml')]

    if 'seeders' in job_list:
        job_list = [
            item.split()[0] for item in seeder_list
        ]

    return job_list


def run_quicker(quicker):
    file_path = f'app/quickers/{quicker}.yaml'
    if not os.path.exists(file_path):
        click.echo(f"Quicker '{file_path}' does not exist.", err=True)
        sys.exit(1)

    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)

    if 'migrations' in config:
        engines, names, fresh = prepare_migration(config)

        for engine in engines:
            run_migrations(engine, names, fresh)

        jprint("✓ Migrations completed", 'success', True)

    seeder_list = []
    if 'seeders' in config:
        engines, seeder_list = prepare_seeders(config)

        for engine in engines:
            run_seeders(seeder_list, engine)

        jprint("✓ Seeders completed", 'success', True)

    if 'jobs' in config:
        the_jobs = prepare_jobs(config, seeder_list)
        run_jobs(the_jobs)
        jprint("✓ Jobs completed", 'success', True)


@click.command(help="Run the specified quicker by name.")
@click.argument("name")
def main(name):
    run_quicker(name)


if __name__ == "__main__":
    main()
