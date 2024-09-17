import sys
import importlib
import argparse


def run_listener(listener_name):
    module_path = f"listeners.{listener_name}"

    try:
        # Dynamically import the job module
        job_module = importlib.import_module(module_path)

        # Call the main function of the imported module
        job_module.main()

    except ModuleNotFoundError:
        print(f"Listener '{listener_name}' not found.")
        sys.exit(1)

    except AttributeError:
        print(f"The listener '{listener_name}' does not have a 'main' function.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Run listener")
    parser.add_argument("job", help="Name of the listener to run")

    args = parser.parse_args()

    run_listener(args.job)


if __name__ == "__main__":
    main()
