import click
import sys
import importlib.metadata
from config.logging import logger
from jetshift_core.commands.dev import main as dev_main
from jetshift_core.commands.make import main as make
from jetshift_core.commands.migration import main as migration
from jetshift_core.commands.seeder import main as seeder
from jetshift_core.commands.job import main as job
from jetshift_core.commands.quicker import main as quicker
from jetshift_core.commands.listener import main as listener

# Banner ASCII art
banner = """
░░░▒█ █▀▀ ▀▀█▀▀ ▒█▀▀▀█ █░░█ ░▀░ █▀▀ ▀▀█▀▀ 
░▄░▒█ █▀▀ ░░█░░ ░▀▀▀▄▄ █▀▀█ ▀█▀ █▀▀ ░░█░░ 
▒█▄▄█ ▀▀▀ ░░▀░░ ▒█▄▄▄█ ▀░░▀ ▀▀▀ ▀░░ ░░▀░░
"""


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """A command-line interface for JetShift."""
    if ctx.invoked_subcommand is None:
        click.echo(banner)

        #  Commands
        click.echo("Commands:")
        commands = ctx.command.list_commands(ctx)
        for command in commands:
            cmd = ctx.command.get_command(ctx, command)
            click.echo(f"  {command:<8} - {cmd.help}")
    else:
        pass


@click.command(help='Show the current version of JetShift.')
def show_version():
    try:
        version = importlib.metadata.version("jetshift-core")
        click.echo(f"JetShift v{version}")
    except (FileNotFoundError, KeyError) as e:
        click.echo(f"Error reading version: {e}", err=True)


# Register Commands
cli.add_command(dev_main, name="dev")
cli.add_command(make, name="make")
cli.add_command(migration, name="migrate")
cli.add_command(seeder, name="seed")
cli.add_command(job, name="job")
cli.add_command(quicker, name="quick")
cli.add_command(listener, name="listen")
cli.add_command(show_version, name="version")


# Main entry point
def main():
    try:
        logger.info("Starting JetShift CLI")
        cli()
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Shutting down JetShift CLI")


if __name__ == "__main__":
    main()
