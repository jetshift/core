from jetshift_core.helpers.quicker import migrations, seeders, jobs
from jetshift_core.helpers.common import jprint


def main():
    migrations_list = ["mysql", "clickhouse"]
    migrations(migrations_list)
    jprint("✓ Migrations completed", 'success', True)

    seeder_list = ["users -n 5"]
    seeders(seeder_list)
    jprint("✓ Seeders completed", 'success', True)

    job_list = ["users"]
    jobs(job_list)
    jprint("✓ Jobs completed", 'success', True)


if __name__ == "__main__":
    main()
