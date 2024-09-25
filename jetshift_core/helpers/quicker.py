import click
from click.testing import CliRunner

runner = CliRunner()


def migrations(engines):
    from jetshift_core.commands.migration import main as run_migration
    for engine in engines:
        result = runner.invoke(run_migration, ['--engine', engine, '--fresh'])
        click.echo(result.output)


def seeders(items):
    from jetshift_core.commands.seeder import main as run_seeder
    for item in items:
        params_split = item.split(' -n ')
        seeder_name = params_split[0]
        engine = 'mysql'

        if len(params_split) > 1:
            records = params_split[1]
            result = runner.invoke(run_seeder, ['--engine', engine, seeder_name, '-n ' + records])
        else:
            result = runner.invoke(run_seeder, ['--engine', engine, seeder_name])

        click.echo(result.output)


def jobs(items):
    from jetshift_core.commands.job import main as run_job
    for item in items:
        result = runner.invoke(run_job, [item])
        click.echo(result.output)
