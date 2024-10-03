import glob
import sys
import os
import click

from jetshift_core.commands.migrations.mysql import migrate as migrate_mysql
from jetshift_core.commands.migrations.clickhouse import migrate as migrate_clickhouse


def run_migration(engine, migration_name, fresh, drop):
    try:
        file_path = f'app/migrations/{migration_name}.yaml'
        if not os.path.exists(file_path):
            click.echo(f"Migration '{file_path}' does not exist.", err=True)
            sys.exit(1)

        click.echo(f"Migrating table: {migration_name}")

        if engine == "mysql":
            migrate_mysql(file_path, fresh, drop)

        elif engine == "clickhouse":
            migrate_clickhouse(file_path, fresh, drop)

        else:
            click.echo(f"Engine '{engine}' is not supported.", err=True)
            sys.exit(1)

        click.echo(f"Migrated table: {migration_name}")
        click.echo("-----")

    except Exception as e:
        click.echo(f"Error migrating table '{migration_name}': {e}", err=True)
        sys.exit(1)


def list_available_migrations():
    package_path = f"app/migrations"

    if not os.path.exists(package_path):
        click.echo(f"Migration directory '{package_path}' does not exist.", err=True)
        sys.exit(1)

    available_migrations = glob.glob(os.path.join(package_path, '*.yaml'))
    migration_names = [os.path.splitext(os.path.basename(migration))[0] for migration in available_migrations]

    return migration_names


def run_all_migrations(engine, fresh, drop):
    available_migrations = list_available_migrations()

    if not available_migrations:
        click.echo("No migrations found.", err=True)
        sys.exit(1)

    for migration_name in available_migrations:
        run_migration(engine, migration_name, fresh, drop)


@click.command(help="Run migrations for a specified database engine.")
@click.argument("migration", required=False, default=None)
@click.option(
    "-e", "--engine", default="mysql", help="Name of the engine (e.g., 'mysql', 'clickhouse'). Default is 'mysql'."
)
@click.option(
    "-f", "--fresh", is_flag=True, help="Truncate the table before running the migration."
)
@click.option(
    "-d", "--drop", is_flag=True, help="Drop the table from the database."
)
def main(migration, engine, fresh, drop):
    click.echo(f"Running migrations for engine '{engine}'")
    click.echo("----------")

    if migration:
        run_migration(engine, migration, fresh, drop)
    else:
        run_all_migrations(engine, fresh, drop)


if __name__ == "__main__":
    main()
