import click
from jetshift_core.helpers.dev import *
from jetshift_core.helpers.common import jprint


@click.command(help="Run the dev environment.")
@click.option(
    "-b", "--background", is_flag=True, help="Run the environment in the background."
)
@click.option(
    "-r", "--reset", is_flag=True, help="Reset the ports if it is already in use."
)
@click.option(
    "-c", "--close", is_flag=True, help="Close the running ports."
)
def main(background, reset, close):
    try:
        if close:
            close_all_ports()
            return

        dev_env(background, reset)
    except PermissionError:
        jprint("Permission denied: 'entrypoint-dev.sh' does not have executable permissions.", 'error')
    except Exception as e:
        jprint(f"An error occurred: {e}", 'error')


if __name__ == "__main__":
    main()
