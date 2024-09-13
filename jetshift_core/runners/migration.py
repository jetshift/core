import sys
import importlib
import argparse
import pkgutil
import os


def list_available_migrations(engine):
    package_path = f"database/migrations/{engine}"

    if not os.path.exists(package_path):
        print(f"Engine '{engine}' not found in the migrations directory.")
        sys.exit(1)

    available_migrations = []

    # Use pkgutil to iterate over the modules in the specified package path
    for _, module_name, is_pkg in pkgutil.iter_modules([package_path]):
        if not is_pkg:
            available_migrations.append(module_name)

    return available_migrations


def run_migration(engine, migration_name, fresh):
    module_path = f"database.migrations.{engine}.{migration_name}"

    try:
        print(f"Migrating: {migration_name} table")
        migration_module = importlib.import_module(module_path)
        sys.argv = [__file__, fresh]

        migration_module.main()
        print(f"Migrated: {migration_name} table")
        print("-----")

    except ModuleNotFoundError:
        print(f"Migration '{migration_name}' under engine '{engine}' not found.")
        sys.exit(1)

    except AttributeError:
        print(f"The migration '{migration_name}' under engine '{engine}' does not have a 'main' function.")
        sys.exit(1)


def run_all_migrations(engine, fresh):
    available_migrations = list_available_migrations(engine)

    if not available_migrations:
        print(f"No migrations found for engine '{engine}'.")
        sys.exit(1)

    for migration_name in available_migrations:
        run_migration(engine, migration_name, fresh)


def main():
    parser = argparse.ArgumentParser(description="Run migrations for a specified database engine.")
    parser.add_argument("engine", help="Database engine (e.g., 'mysql', 'clickhouse')")
    parser.add_argument("migration", nargs='?', default=None, help="Name of the migration to run or omit to run all migrations")
    parser.add_argument("fresh", nargs='?', default=None, help="Truncate tables during migrations")

    args = parser.parse_args()

    # print(args)
    # return False

    print(f"Running migrations for engine '{args.engine}'")
    print("----------")

    if args.migration:
        run_migration(args.engine, args.migration, args.fresh)
    else:
        run_all_migrations(args.engine, args.fresh)


if __name__ == "__main__":
    main()
