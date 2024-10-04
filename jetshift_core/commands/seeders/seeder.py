import glob
import sys
import os
import click
from jetshift_core.commands.seeders.mysql import seed_mysql
from jetshift_core.commands.seeders.clickhouse import seed_clickhouse


def run_seeder(seeder_engine, seeder_name, records, dependent_records, skip_dependencies, skip_dependencies_if_data_exists):
    try:
        click.echo(f"Running seeder: {seeder_name}")

        if seeder_engine == "mysql":
            seed_mysql(seeder_engine, seeder_name, records, dependent_records, skip_dependencies, skip_dependencies_if_data_exists)

        elif seeder_engine == "clickhouse":
            seed_clickhouse(seeder_engine, seeder_name, records, dependent_records, skip_dependencies, skip_dependencies_if_data_exists)

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
@click.option("-nd", default=5, help="Number of records to seed for dependencies. Default is 5.")
@click.option(
    "-sd", is_flag=True, default=False, help="Skip dependent seeders. Default is False."
)
@click.option(
    "-sdd", is_flag=True, default=False, help="Skip dependent seeders if data already exists. Default is False."
)
def main(engine, seeder, n, nd, sd, sdd):
    if seeder is None:
        seeder_list = [os.path.splitext(os.path.basename(file))[0] for file in glob.glob('app/migrations/*.yaml')]
        if not seeder_list:
            click.echo("No seeders found.")
            return

        for seeder_name in seeder_list:
            run_seeder(engine, seeder_name, n, nd, sd, sdd)
    else:
        run_seeder(engine, seeder, n, nd, sd, sdd)


if __name__ == "__main__":
    main()
