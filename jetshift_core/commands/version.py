import importlib.metadata


# @click.command(help='Show the current version of JetShift.')
def show_version():
    try:
        version = importlib.metadata.version("jetshift-core")
        return f"v{version}\n"
        pass
    except (FileNotFoundError, KeyError) as e:
        return f"Error reading version: {e}"


if __name__ == "__main__":
    show_version()
