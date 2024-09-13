import sys
import importlib
import os
import argparse
from pathlib import Path


def run_seeder(seeder_type, seeder_name, seeder_args):
    module_path = f"database.seeders.{seeder_type}.{seeder_name}"

    try:
        # Dynamically import the seeder module
        seeder_module = importlib.import_module(module_path)

        # Call the main function of the imported module with additional arguments
        seeder_module.main(*seeder_args)

    except ModuleNotFoundError:
        print(f"Seeder '{seeder_name}' not found. Please check the seeder type.")
        sys.exit(1)

    except AttributeError:
        print(f"The seeder '{seeder_name}' does not have a 'main' function.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Run seeders.")
    parser.add_argument("type", help="Name of the type (e.g., 'mysql', 'csv')")
    parser.add_argument("seeder", nargs="?", default=None, help="Name of the seeder to run (or leave empty to run all seeders)")
    parser.add_argument("seeder_args", nargs=argparse.REMAINDER, help="Additional arguments for the seeder")

    args = parser.parse_args()

    if args.seeder is None:
        # Get the root directory of the project
        project_root = Path(__file__).parent.parent.parent
        seeders_dir = os.path.join(project_root, 'database', 'seeders', args.type)

        for filename in os.listdir(seeders_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                seeder_name = filename[:-3]  # Remove .py extension
                print(f"Running seeder: {seeder_name}")
                run_seeder(args.type, seeder_name, args.seeder_args)
    else:
        run_seeder(args.type, args.seeder, args.seeder_args)


if __name__ == "__main__":
    main()
