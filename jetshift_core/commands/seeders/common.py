def find_dependencie1s(engine, table_name, num_records, visited=None):
    from jetshift_core.helpers.mysql import get_mysql_table_definition

    if visited is None:
        visited = set()  # Set to keep track of visited tables and prevent cycles

    try:
        # Add the current table to the visited set
        visited.add(table_name)

        # Get the table definition and its dependencies
        table = get_mysql_table_definition(table_name)
        dependencies = table.info.get('dependencies', [])

        # Recursively find dependencies
        all_dependencies = set()
        for dependency in dependencies:
            if dependency not in visited:
                print(dependency)
                all_dependencies.add(dependency)
                # Collect dependencies of the dependency recursively
                child_dependencies = find_dependencies(engine, dependency, num_records, visited)
                all_dependencies.update(child_dependencies)

        print(all_dependencies)
        return list(all_dependencies)
    except Exception as e:
        print("Error finding dependencies for table '%s': %s", table_name, e)
        return []  # Return an empty list if an error occurs


def find_dependencies(engine, table_name, num_records, visited=None, dependency_order=None):
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
            dependency_order[table_name] = {"order": len(dependency_order) + 1}

            # Retrieve table definition
            table = get_mysql_table_definition(table_name)
            dependencies = table.info.get('dependencies', [])

            # print(dependencies)

            # Recursively find dependencies
            for dependency in dependencies:
                if dependency not in visited:
                    # Recursively call for each dependency
                    find_dependencies(engine, dependency, num_records, visited, dependency_order)

        return dependency_order
    except Exception as e:
        print(f"Error finding dependencies for table '{table_name}': {e}")
        return {}

# Example usage
# Uncomment the lines below to run with an actual engine and table name
# engine = "mock_engine_instance"
# print(find_dependencies(engine, 'orders', 10))


def min_max_id(engine, table_name):
    if engine == 'mysql':
        from jetshift_core.helpers.mysql import get_min_max_id
        return get_min_max_id(table_name)

    elif engine == 'clickhouse':
        from jetshift_core.helpers.clcikhouse import get_min_max_id
        return get_min_max_id(table_name)
