import sys
import importlib
import click


def run_quicker(job_name):
    module_path = f"quickers.{job_name}"

    try:
        # Dynamically import the module
        job_module = importlib.import_module(module_path)

        # Call the main function of the imported module
        job_module.main()

    except ModuleNotFoundError:
        click.echo(f"Quicker '{job_name}' not found.", err=True)
        sys.exit(1)

    except AttributeError:
        click.echo(f"The quicker '{job_name}' does not have a 'main' function.", err=True)
        sys.exit(1)


@click.command(help="Run the specified quicker by name.")
@click.argument("name")
def main(name):
    run_quicker(name)


if __name__ == "__main__":
    main()
