import os
import shutil
import argparse
from pathlib import Path

from jetshift_core.helpers.common import to_pascal_case


def make_migration(engine, new_migration_name):
    # Get the directory of stub files
    stub_root = Path(__file__).parent
    stub_dir = os.path.join(stub_root, 'migrations')
    stub_path = os.path.join(stub_dir, engine + '.py')

    migration_path = os.path.join(os.getcwd(), 'database', 'migrations', engine, new_migration_name + '.py')

    # Check if the specified engine exists in the available engines
    available_engines = [os.path.splitext(f)[0] for f in os.listdir(stub_dir) if f.endswith('.py')]
    if engine not in available_engines:
        print(f"Error: The specified engine '{engine}' does not exist in available engines.")
        print(f"Available engines: {', '.join(available_engines)}")
        return

    # Check if the migration file already exists
    if os.path.exists(migration_path):
        print(f"Migration '{new_migration_name}' already exists at. Please choose a different name.")
        return

    # Copy stub file to new location
    shutil.copy(stub_path, migration_path)

    # Open the new file and modify its content
    with open(migration_path, 'r') as file:
        content = file.read()

    # Replace placeholder table name with the new table name
    content = content.replace('table_name', new_migration_name)

    # Write the updated content back to the file
    with open(migration_path, 'w') as file:
        file.write(content)

    print(f"Migration {new_migration_name} created successfully.")


def make_seeder(engine, new_seeder_name):
    # Get the directory of stub files
    stub_root = Path(__file__).parent
    stub_dir = os.path.join(stub_root, 'seeders')
    stub_path = os.path.join(Path(__file__).parent, 'seeders', engine + '.py')

    seeder_path = os.path.join(os.getcwd(), 'database', 'seeders', engine, new_seeder_name + '.py')

    # Check if the specified engine exists in the available engines
    available_engines = [os.path.splitext(f)[0] for f in os.listdir(stub_dir) if f.endswith('.py')]
    if engine not in available_engines:
        print(f"Error: The specified engine '{engine}' does not exist in available engines.")
        print(f"Available engines: {', '.join(available_engines)}")
        return

    # Check if the migration file already exists
    if os.path.exists(seeder_path):
        print(f"Seeder '{new_seeder_name}' already exists at. Please choose a different name.")
        return

    # Copy stub file to new location
    shutil.copy(stub_path, seeder_path)

    # Open the new file and modify its content
    with open(seeder_path, 'r') as file:
        content = file.read()

    # Replace placeholder table name with the new table name
    content = content.replace('the_table_name', new_seeder_name)

    # Write the updated content back to the file
    with open(seeder_path, 'w') as file:
        file.write(content)

    print(f"Seeder '{new_seeder_name}' created successfully.")


def make_job(new_job_name, jobtype):
    # Get the directory of stub files
    stub_root = Path(__file__).parent
    stub_dir = os.path.join(stub_root, 'jobs')
    stub_path = os.path.join(stub_dir, jobtype + '.py')

    job_path = os.path.join(os.getcwd(), 'jobs', new_job_name + '.py')

    # Check if the specified engine exists in the available engines
    available_job_types = [os.path.splitext(f)[0] for f in os.listdir(stub_dir) if f.endswith('.py')]
    if jobtype not in available_job_types:
        print(f"Error: The specified job type '{jobtype}' does not exist in available job types.")
        print(f"Available job types: {', '.join(available_job_types)}")
        return

    # Check if the migration file already exists
    if os.path.exists(job_path):
        print(f"Job '{new_job_name}' already exists. Please choose a different name.")
        return

    # Copy stub file to new location
    shutil.copy(stub_path, job_path)

    # Open the new file and modify its content
    with open(job_path, 'r') as file:
        content = file.read()

    # Replace placeholder table name with the new table name
    content = content.replace('the_table_name', new_job_name)
    content = content.replace('job_class_name', to_pascal_case(new_job_name) + 'Job')

    # Write the updated content back to the file
    with open(job_path, 'w') as file:
        file.write(content)

    print(f"Job {new_job_name} created successfully.")


def make_quicker(new_quicker_name):
    # Get the directory of stub files
    stub_root = Path(__file__).parent
    stub_dir = os.path.join(stub_root, 'quickers')
    stub_path = os.path.join(stub_dir, 'mix.py')

    quicker_path = os.path.join(os.getcwd(), 'quickers', new_quicker_name + '.py')

    # Check if the migration file already exists
    if os.path.exists(quicker_path):
        print(f"Quicker '{new_quicker_name}' already exists. Please choose a different name.")
        return

    # Copy stub file to new location
    shutil.copy(stub_path, quicker_path)

    # Open the new file and modify its content
    with open(quicker_path, 'r') as file:
        content = file.read()

    # Write the updated content back to the file
    with open(quicker_path, 'w') as file:
        file.write(content)

    print(f"Quicker {new_quicker_name} created successfully.")


def main():
    parser = argparse.ArgumentParser(description="Create from stubs")
    parser.add_argument("type", help="Name of the type (e.g., 'migration', 'seeder', 'job)")
    parser.add_argument("name", help="Name of the new file")

    # Optional argument with a default value
    parser.add_argument(
        "-e", "--engine",
        help="Name of the engine (e.g., 'mysql', 'clickhouse'). Default is 'mysql'.",
        default="mysql"
    )
    parser.add_argument(
        "-jt", "--jobtype",
        help="Type of the job (e.g., 'simple', 'mysql_clickhouse'). Default is 'simple'.",
        default="simple"
    )

    args = parser.parse_args()

    if args.type == "migration":
        make_migration(args.engine, args.name)
    elif args.type == "seeder":
        make_seeder(args.engine, args.name)
    elif args.type == "job":
        make_job(args.name, args.jobtype)
    elif args.type == "quicker":
        make_quicker(args.name)
    else:
        print("Invalid type. Must be 'migration', 'seeder' or 'job'")


if __name__ == "__main__":
    main()
