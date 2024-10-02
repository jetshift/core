import sys
import importlib
import click


def run_listener(listener_name):
    module_path = f"app.listeners.{listener_name}"

    try:
        # Dynamically import the listener module
        listener_module = importlib.import_module(module_path)

        # Call the main function of the imported module
        listener_module.main()

    except ModuleNotFoundError:
        click.echo(f"Listener '{listener_name}' not found.", err=True)
        sys.exit(1)

    except AttributeError:
        click.echo(f"The listener '{listener_name}' does not have a 'main' function.", err=True)
        sys.exit(1)


@click.command(help="Run the specified listener by name.")
@click.argument("listener")
def main(listener):
    run_listener(listener)


if __name__ == "__main__":
    main()  # Click automatically handles argument parsing
