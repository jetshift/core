def find_dependencies(engine, table_name, dependent_records, visited=None, dependency_order=None):
    from jetshift_core.helpers.mysql import get_mysql_table_definition

    # Initialize visited and dependency_order if they are not already provided
    if visited is None:
        visited = set()
    if dependency_order is None:
        dependency_order = {}  # Dictionary to keep track of dependency addition order

    try:
        # Check if table is already visited to avoid cycles
        if table_name not in visited:
            # Mark the table as visited
            visited.add(table_name)

            # Record the order and metadata for the table
            dependency_order[table_name] = {
                "order": len(dependency_order) + 1,
                "dependent_records": dependent_records,
            }

            # Retrieve table definition
            table = get_mysql_table_definition(table_name)
            dependencies = table.info.get('dependencies', [])

            # Recursively find dependencies
            for dependency in dependencies:
                if dependency not in visited:
                    # Recursively call for each dependency
                    find_dependencies(engine, dependency, dependent_records, visited, dependency_order)

        return dependency_order
    except Exception as e:
        print(f"Error finding dependencies for table '{table_name}': {e}")
        return {}


def table_has_data(engine, table_name):
    if engine == 'mysql':
        from jetshift_core.helpers.mysql import check_table_has_data
        return check_table_has_data(table_name)

    elif engine == 'clickhouse':
        from jetshift_core.helpers.clcikhouse import check_table_has_data
        return check_table_has_data(table_name)


def min_max_id(engine, table_name):
    if engine == 'mysql':
        from jetshift_core.helpers.mysql import get_min_max_id
        return get_min_max_id(table_name)

    elif engine == 'clickhouse':
        from jetshift_core.helpers.clcikhouse import get_min_max_id
        return get_min_max_id(table_name)
