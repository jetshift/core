def min_max_id(engine, table_name):
    if engine == 'mysql':
        from jetshift_core.helpers.mysql import get_min_max_id
        return get_min_max_id(table_name)

    elif engine == 'clickhouse':
        from jetshift_core.helpers.clcikhouse import get_min_max_id
        return get_min_max_id(table_name)
