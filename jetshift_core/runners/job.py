import sys
import importlib
import argparse


def run_job(job_name):
    module_path = f"jobs.{job_name}"

    try:
        # Dynamically import the job module
        job_module = importlib.import_module(module_path)

        # Call the main function of the imported module
        job_module.main()

    except ModuleNotFoundError:
        print(f"Job '{job_name}' not found.")
        sys.exit(1)

    except AttributeError:
        print(f"The job '{job_name}' does not have a 'main' function.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Run jobs.")
    parser.add_argument("job", help="Name of the job to run (e.g., 'time', 'other_job')")

    args = parser.parse_args()

    run_job(args.job)


if __name__ == "__main__":
    main()
