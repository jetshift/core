import sys
import importlib
import click


def run_job(job_name):
    module_path = f"jobs.{job_name}"

    try:
        # Dynamically import the job module
        job_module = importlib.import_module(module_path)
        job_module.main()

    except ModuleNotFoundError:
        click.echo(f"Job '{job_name}' not found.", err=True)
        sys.exit(1)

    except AttributeError:
        click.echo(f"The job '{job_name}' does not have a 'main' function.", err=True)
        sys.exit(1)


@click.command(help="Run specified jobs by name.")
@click.argument("job")
def main(job):
    run_job(job)


if __name__ == "__main__":
    main()
