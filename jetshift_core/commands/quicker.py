import glob
import os
import sys
import yaml
import click
from jetshift_core.helpers.quicker import migrations, seeders, jobs
from jetshift_core.helpers.common import jprint


def prepare_seeders(config):
    seeder_list = config.get('seeders', [])

    if 'all' in seeder_list:
        seeder_list = [os.path.splitext(os.path.basename(file))[0] for file in glob.glob('database/migrations/*.yaml')]

    return seeder_list


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
        migration_list = config['migrations'].get('engines', [])
        fresh = config['migrations'].get('fresh', True)

        migrations(migration_list, fresh)
        jprint("✓ Migrations completed", 'success', True)

    seeder_list = []
    if 'seeders' in config:
        seeder_list = prepare_seeders(config)
        seeders(seeder_list)
        jprint("✓ Seeders completed", 'success', True)

    if 'jobs' in config:
        the_jobs = prepare_jobs(config, seeder_list)
        jobs(the_jobs)
        jprint("✓ Jobs completed", 'success', True)


@click.command(help="Run the specified quicker by name.")
@click.argument("name")
def main(name):
    run_quicker(name)


if __name__ == "__main__":
    main()
