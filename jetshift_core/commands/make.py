import os
import shutil
import click
from pathlib import Path

from jetshift_core.helpers.common import to_pascal_case, jprint


def make_migration(engine, new_migration_name):
    # Get the directory of stub files
    stub_root = os.path.join(Path(__file__).parent.parent, 'stubs')
    stub_dir = os.path.join(stub_root, 'migrations')
    stub_path = os.path.join(stub_dir, 'sample.yaml')

    migration_path = os.path.join(os.getcwd(), 'app', 'migrations', new_migration_name + '.yaml')

    # Check if the migration file already exists
    if os.path.exists(migration_path):
        error_msg = f"Migration [{migration_path}] already exists."
        jprint(error_msg, 'error')
        return

    # Copy stub file to new location
    shutil.copy(stub_path, migration_path)

    # Open the new file and modify its content
    with open(migration_path, 'r') as file:
        content = file.read()

    # Replace placeholder table name with the new table name
    content = content.replace('TableName', new_migration_name)

    # Write the updated content back to the file
    with open(migration_path, 'w') as file:
        file.write(content)

    success_msg = f"Migration [{migration_path}] created successfully."
    jprint(success_msg, 'info')


def make_job(new_job_name, jobtype):
    # Get the directory of stub files
    stub_root = os.path.join(Path(__file__).parent.parent, 'stubs')
    stub_dir = os.path.join(stub_root, 'jobs')
    stub_path = os.path.join(stub_dir, jobtype + '.yaml')

    job_path = os.path.join(os.getcwd(), 'app', 'jobs', new_job_name + '.yaml')

    # Check if the migration file already exists
    if os.path.exists(job_path):
        jprint(f"Job [{job_path}] already exists.", 'error')
        return

    # Copy stub file to new location
    shutil.copy(stub_path, job_path)

    # Open the new file and modify its content
    with open(job_path, 'r') as file:
        content = file.read()

    # Replace placeholder table name with the new table name
    content = content.replace('TableName', new_job_name)
    # content = content.replace('job_class_name', to_pascal_case(new_job_name) + 'Job')

    # Write the updated content back to the file
    with open(job_path, 'w') as file:
        file.write(content)

    jprint(f"Job [{job_path}] created successfully.", 'info')


def make_quicker(new_quicker_name):
    # Get the directory of stub files
    stub_root = os.path.join(Path(__file__).parent.parent, 'stubs')
    stub_dir = os.path.join(stub_root, 'quickers')
    stub_path = os.path.join(stub_dir, 'sample.yaml')

    quicker_path = os.path.join(os.getcwd(), 'app', 'quickers', new_quicker_name + '.yaml')

    # Check if the migration file already exists
    if os.path.exists(quicker_path):
        jprint(f"Quicker [{quicker_path}] already exists.", 'error')
        return

    # Copy stub file to new location
    shutil.copy(stub_path, quicker_path)

    # Open the new file and modify its content
    with open(quicker_path, 'r') as file:
        content = file.read()

    # Write the updated content back to the file
    with open(quicker_path, 'w') as file:
        file.write(content)

    jprint(f"Quicker [{quicker_path}] created successfully.", 'info')


@click.command(help="Make a new migration, seeder, job, or quicker.")
@click.argument("type")
@click.argument("name")
@click.option(
    "-e", "--engine", default="mysql", help="Name of the engine (e.g., 'mysql', 'clickhouse'). Default is 'mysql'."
)
@click.option(
    "-jt", "--jobtype", default="common", help="Type of the job (e.g., 'simple', 'mysql_clickhouse'). Default is 'simple'."
)
def main(type, name, engine, jobtype):
    if type == "migration":
        make_migration(engine, name)
    elif type == "job":
        make_job(name, jobtype)
    elif type == "quicker":
        make_quicker(name)
    else:
        jprint("Invalid make type. Must be 'migration', 'job', or 'quicker'.", 'error')


if __name__ == "__main__":
    main()
