import sys
import importlib
import click
from jetshift_core.helpers.common import jprint


def run_yaml_job(job_name):
    import os
    import yaml
    import importlib
    from config.luigi import luigi, local_scheduler

    # Load configuration from YAML file
    file_path = f'app/jobs/{job_name}.yaml'

    if not os.path.exists(file_path):
        jprint(f"Job config file '{file_path}' not found.", 'error')
        sys.exit(1)

    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)

    # Dynamically import the specified class
    module_name = config['task']['module']
    module = importlib.import_module(module_name)
    base_task = getattr(module, 'BaseTask')

    class RunJob(base_task):
        table_name = config['table'].get('name')

    # Extract parameters from the config
    params = {
        'live_schema': config['table'].get('live_schema', False),
        'primary_id': config['table'].get('primary_id', None),
        'extract_offset': config['extract'].get('offset', 0),
        'extract_limit': config['extract'].get('limit', 0),
        'extract_chunk_size': config['extract'].get('chunk_size', 100),
        'truncate_table': config['load'].get('truncate_table', False),
        'load_chunk_size': config['load'].get('chunk_size', 100),
        'sleep_interval': config['load'].get('sleep_interval', 1),
    }

    # Build and run the Luigi task with the parameters from the YAML config
    luigi.build([RunJob(**params)], local_scheduler=local_scheduler)


def run_py_job(job_name):
    from jetshift_core.helpers.common import run_command_subprocess

    try:
        module_path = f"app.jobs.{job_name}"

        # Dynamically import the job module
        job_module = importlib.import_module(module_path)
        job_module.main()

        # command = [sys.executable, '-m', module_path]
        # run_command_subprocess(command)

    except ModuleNotFoundError:
        click.echo(f"Job '{job_name}' not found.", err=True)
        sys.exit(1)

    except AttributeError:
        click.echo(f"The job '{job_name}' does not have a 'main' function.", err=True)
        sys.exit(1)


def run_job(job_name, type):
    if type == "yaml":
        run_yaml_job(job_name)
    elif type == "py":
        run_py_job(job_name)
    else:
        click.echo(f"Invalid job type '{type}'.", err=True)
        sys.exit(1)

    return False


@click.command(help="Run specified jobs by name.")
@click.argument("job")
@click.option(
    "-t", "--type", default="yaml", help="Type of the job ('yaml', 'py'). Default is 'yaml'."
)
def main(job, type):
    run_job(job, type)


if __name__ == "__main__":
    main()
