def migrate_supported_pairs(source, target, check=False):
    supported_pairs = [
        {
            'source': {'title': 'MySQL', 'dialect': 'mysql'},
            'target': {'title': 'ClickHouse', 'dialect': 'clickhouse'},
            'task_path': 'jetshift_core.tasks.migrate.mysql_clickhouse.mysql_to_clickhouse_flow'
        },
        {
            'source': {'title': 'PostgreSQL', 'dialect': 'postgresql'},
            'target': {'title': 'ClickHouse', 'dialect': 'clickhouse'},
            'task_path': 'jetshift_core.tasks.postgres_clickhouse_insert.PostgreToClickhouse'
        },
        {
            'source': {'title': 'SQLite', 'dialect': 'sqlite'},
            'target': {'title': 'ClickHouse', 'dialect': 'clickhouse'},
            'task_path': 'jetshift_core.tasks.sqlite_clickhouse_insert.SqliteToClickhouse'
        },
    ]

    if not check:
        return supported_pairs

    # Check if the given source and target match any pair
    for pair in supported_pairs:
        if pair['source']['dialect'] == source and pair['target']['dialect'] == target:
            success = True
            message = f"The pair '{source} -> {target}' is supported by JetShift"
            return success, message, pair['task_path']

    success = False
    message = f"Unsupported migration pair: {source} -> {target}"
    return success, message, None


class AttrDict(dict):
    """Dictionary with attribute-style access that returns None for missing keys."""

    def __getattr__(self, item):
        return self.get(item, None)  # Return None if the key is missing

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, item):
        if item in self:
            del self[item]
