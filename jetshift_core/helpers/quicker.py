import click
from click.testing import CliRunner

runner = CliRunner()


def run_migrations(engine, names, fresh):
    from jetshift_core.commands.migrations.migration import main as run_migration

    if names:
        for name in names:
            result = runner.invoke(run_migration, [name, '--engine', engine] + (['--fresh'] if fresh else []))
            click.echo(result.output)
    else:
        result = runner.invoke(run_migration, ['--engine', engine] + (['--fresh'] if fresh else []))
        click.echo(result.output)


def run_seeders(items, engine='mysql'):
    from jetshift_core.commands.seeders.seeder import main as run_seeder
    for item in items:
        params_split = item.split(' -n ')
        seeder_name = params_split[0]

        if len(params_split) > 1:
            records = params_split[1]
            result = runner.invoke(run_seeder, ['--engine', engine, seeder_name, '-n ' + records])
        else:
            result = runner.invoke(run_seeder, ['--engine', engine, seeder_name])

        click.echo(result.output)


def run_jobs(items):
    from jetshift_core.commands.job import main as run_job
    for item in items:
        result = runner.invoke(run_job, [item])
        click.echo(result.output)
