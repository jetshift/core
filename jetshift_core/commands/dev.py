import click
from jetshift_core.helpers.dev import dev_env


@click.command(help="Run the dev environment.")
@click.argument("reset_port", required=False, default=None)
def main(reset_port=None):
    try:
        dev_env(reset_port)
    except FileNotFoundError:
        click.echo("The script 'entrypoint-dev.sh' was not found.", err=True)
    except PermissionError:
        click.echo("Permission denied: 'entrypoint-dev.sh' does not have executable permissions.", err=True)
    except Exception as e:
        click.echo(f"An error occurred: {e}", err=True)


if __name__ == "__main__":
    main()
