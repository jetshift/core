import sys
import importlib
import pkgutil
import os
import click


def list_available_migrations(engine):
    package_path = f"database/migrations/{engine}"

    if not os.path.exists(package_path):
        click.echo(f"Engine '{engine}' not found in the migrations directory.", err=True)
        sys.exit(1)

    available_migrations = []

    # Use pkgutil to iterate over the modules in the specified package path
    for _, module_name, is_pkg in pkgutil.iter_modules([package_path]):
        if not is_pkg:
            available_migrations.append(module_name)

    return available_migrations


def run_migration(engine, migration_name, fresh):
    module_path = f"database.migrations.{engine}.{migration_name}"
    file_path = os.path.join("database", "migrations", engine, f"{migration_name}.py")

    if not os.path.exists(file_path):
        click.echo(f"Migration file '{file_path}' does not exist.", err=True)
        sys.exit(1)

    try:
        click.echo(f"Migrating table: {migration_name}")
        migration_module = importlib.import_module(module_path)

        # Call the Click command directly as a function, passing 'fresh' as an argument
        migration_module.main(fresh)

        click.echo(f"Migrated table: {migration_name}")
        click.echo("-----")

    except ModuleNotFoundError:
        click.echo(f"Migration '{migration_name}' under engine '{engine}' not found.", err=True)
        sys.exit(1)

    except AttributeError:
        click.echo(f"The migration '{migration_name}' under engine '{engine}' does not have a 'main' function.", err=True)
        sys.exit(1)


def run_all_migrations(engine, fresh):
    available_migrations = list_available_migrations(engine)

    if not available_migrations:
        click.echo(f"No migrations found for engine '{engine}'.", err=True)
        sys.exit(1)

    for migration_name in available_migrations:
        run_migration(engine, migration_name, fresh)


@click.command(help="Run migrations for a specified database engine.")
@click.argument("migration", required=False, default=None)
@click.option(
    "-e", "--engine", default="mysql", help="Name of the engine (e.g., 'mysql', 'clickhouse'). Default is 'mysql'."
)
@click.option(
    "-f", "--fresh", is_flag=True, help="Truncate the table before running the migration."
)
def main(migration, engine, fresh):
    click.echo(f"Running migrations for engine '{engine}'")
    click.echo("----------")

    if migration:
        run_migration(engine, migration, fresh)
    else:
        run_all_migrations(engine, fresh)


if __name__ == "__main__":
    main()
