import sys
import importlib
import os
import click

from jetshift_core.commands.seeders.mysql import seed_mysql


def run_seeder(seeder_engine, seeder_name, records):
    # from click.testing import CliRunner
    try:
        click.echo(f"Running seeder: {seeder_name}")

        if seeder_engine == "mysql":
            seed_mysql(seeder_name, records)

        else:
            click.echo(f"Seeder engine '{seeder_engine}' is not supported.", err=True)
            sys.exit(1)

    except ModuleNotFoundError:
        click.echo(f"Seeder '{seeder_name}' not found. Please check the seeder engine.", err=True)
        sys.exit(1)

    except AttributeError:
        click.echo(f"The seeder '{seeder_name}' does not have a 'main' function.", err=True)
        sys.exit(1)


@click.command(help="Run seeders for a specified engine. You can run a specific seeder or all seeders in a engine.")
@click.argument("seeder", required=False, default=None)
@click.option(
    "-e", "--engine", default="mysql", help="Name of the engine (e.g., 'mysql', 'clickhouse'). Default is 'mysql'."
)
@click.option("-n", default=10, help="Number of records to seed. Default is 10.")
def main(engine, seeder, n):
    if seeder is None:
        # Get the root directory of the project
        seeders_dir = os.path.join(os.getcwd(), 'database', 'seeders', engine)

        if not os.path.exists(seeders_dir):
            click.echo(f"No seeders found for engine '{engine}'.", err=True)
            sys.exit(1)

        for filename in os.listdir(seeders_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                seeder_name = filename[:-3]  # Remove .py extension
                run_seeder(engine, seeder_name, n)
    else:
        run_seeder(engine, seeder, n)


if __name__ == "__main__":
    main()
