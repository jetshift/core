import sys
import importlib
import argparse


def run_quicker(job_name):
    module_path = f"quickers.{job_name}"

    try:
        # Dynamically import the module
        job_module = importlib.import_module(module_path)

        # Call the main function of the imported module
        job_module.main()

    except ModuleNotFoundError:
        print(f"Quicker '{job_name}' not found.")
        sys.exit(1)

    except AttributeError:
        print(f"The quicker '{job_name}' does not have a 'main' function.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Run quicker")
    parser.add_argument("name", help="Name of the quicker")

    args = parser.parse_args()

    run_quicker(args.name)


if __name__ == "__main__":
    main()
