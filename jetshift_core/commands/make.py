import os
import shutil
import click
from pathlib import Path

from jetshift_core.helpers.common import to_pascal_case, jprint


def make_migration(engine, new_migration_name):
    # Get the directory of stub files
    stub_root = os.path.join(Path(__file__).parent.parent, 'stubs')
    stub_dir = os.path.join(stub_root, 'migrations')
    stub_path = os.path.join(stub_dir, engine + '.py')

    migration_path = os.path.join(os.getcwd(), 'database', 'migrations', engine, new_migration_name + '.py')

    # Check if the specified engine exists in the available engines
    available_engines = [os.path.splitext(f)[0] for f in os.listdir(stub_dir) if f.endswith('.py')]
    if engine not in available_engines:
        error_msg = f"The specified engine '{engine}' does not exist in available engines. Available engines: {', '.join(available_engines)}"
        jprint(error_msg, 'error')
        return

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
    content = content.replace('table_name', new_migration_name)

    # Write the updated content back to the file
    with open(migration_path, 'w') as file:
        file.write(content)

    success_msg = f"Migration [{migration_path}] created successfully."
    jprint(success_msg, 'info')


def make_seeder(engine, new_seeder_name):
    # Get the directory of stub files
    stub_root = os.path.join(Path(__file__).parent.parent, 'stubs')
    stub_dir = os.path.join(stub_root, 'seeders')
    stub_path = os.path.join(stub_dir, engine + '.py')

    seeder_path = os.path.join(os.getcwd(), 'database', 'seeders', engine, new_seeder_name + '.py')

    # Check if the specified engine exists in the available engines
    available_engines = [os.path.splitext(f)[0] for f in os.listdir(stub_dir) if f.endswith('.py')]
    if engine not in available_engines:
        error_msg = f"The specified engine '{engine}' does not exist in available engines. Available engines: {', '.join(available_engines)}"
        jprint(error_msg, 'error')
        return

    # Check if the migration file already exists
    if os.path.exists(seeder_path):
        jprint(f"Seeder [{seeder_path}] already exists.", 'error')
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

    jprint(f"Seeder [{seeder_path}] created successfully.", 'info')


def make_job(new_job_name, jobtype):
    # Get the directory of stub files
    stub_root = os.path.join(Path(__file__).parent.parent, 'stubs')
    stub_dir = os.path.join(stub_root, 'jobs')
    stub_path = os.path.join(stub_dir, jobtype + '.py')

    job_path = os.path.join(os.getcwd(), 'jobs', new_job_name + '.py')

    # Check if the specified engine exists in the available engines
    available_job_types = [os.path.splitext(f)[0] for f in os.listdir(stub_dir) if f.endswith('.py')]
    if jobtype not in available_job_types:
        error_msg = f"The specified job type '{jobtype}' does not exist in available job types. Available job types: {', '.join(available_job_types)}"
        jprint(error_msg, 'error')
        return

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
    content = content.replace('the_table_name', new_job_name)
    content = content.replace('job_class_name', to_pascal_case(new_job_name) + 'Job')

    # Write the updated content back to the file
    with open(job_path, 'w') as file:
        file.write(content)

    jprint(f"Job [{job_path}] created successfully.", 'info')


def make_quicker(new_quicker_name):
    # Get the directory of stub files
    stub_root = os.path.join(Path(__file__).parent.parent, 'stubs')
    stub_dir = os.path.join(stub_root, 'quickers')
    stub_path = os.path.join(stub_dir, 'mix.py')

    quicker_path = os.path.join(os.getcwd(), 'quickers', new_quicker_name + '.py')

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
    "-jt", "--jobtype", default="simple", help="Type of the job (e.g., 'simple', 'mysql_clickhouse'). Default is 'simple'."
)
def main(type, name, engine, jobtype):
    if type == "migration":
        make_migration(engine, name)
    elif type == "seeder":
        make_seeder(engine, name)
    elif type == "job":
        make_job(name, jobtype)
    elif type == "quicker":
        make_quicker(name)
    else:
        jprint("Invalid make type. Must be 'migration', 'seeder', 'job', or 'quicker'.", 'error')


if __name__ == "__main__":
    main()
