import subprocess
import click


@click.command(help="Run the dev environment.")
def main():
    try:
        # Use 'bash' instead of 'sh' if required for your shell
        subprocess.call(['sh', 'entrypoint-dev.sh'])
    except FileNotFoundError:
        click.echo("The script 'entrypoint-dev.sh' was not found.", err=True)
    except PermissionError:
        click.echo("Permission denied: 'entrypoint-dev.sh' does not have executable permissions.", err=True)
    except Exception as e:
        click.echo(f"An error occurred: {e}", err=True)


if __name__ == "__main__":
    main()
