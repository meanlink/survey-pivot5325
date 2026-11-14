from sqlalchemy import inspect, text
from src.config.database import Base, engine
from src import models

# Additive columns so an existing deployment keeps its old archives while
# gaining result-package semantics. Both PostgreSQL and SQLite accept these.
DELIVERY_COLUMNS = {
    'scheduled_for': 'DOUBLE PRECISION',
    'completed_at': 'DOUBLE PRECISION',
    'last_attempt_at': 'DOUBLE PRECISION',
    'status': "VARCHAR(20) DEFAULT 'complete'",
    'csv_filename': 'VARCHAR(200)',
    'json_filename': 'VARCHAR(200)',
    'csv_checksum': 'VARCHAR(64)',
    'json_checksum': 'VARCHAR(64)',
    'snapshot': 'JSON',
    'error': 'VARCHAR(500)',
}


def migrate():
    Base.metadata.create_all(engine)
    inspector = inspect(engine)
    if 'deliveries' not in inspector.get_table_names():
        return
    existing = {column['name'] for column in inspector.get_columns('deliveries')}
    with engine.begin() as connection:
        for name, definition in DELIVERY_COLUMNS.items():
            if name not in existing:
                connection.execute(text(f'ALTER TABLE deliveries ADD COLUMN {name} {definition}'))
        # At most one archived package per report per planned due time. NULL
        # scheduled_for marks pre-upgrade rows, which are exempt.
        constraints = {c['name'] for c in inspector.get_unique_constraints('deliveries')}
        indexes = {i['name'] for i in inspector.get_indexes('deliveries')}
        if 'uq_delivery_run' not in constraints and 'uq_delivery_run' not in indexes:
            connection.execute(text(
                'CREATE UNIQUE INDEX IF NOT EXISTS uq_delivery_run ON deliveries (report_id, scheduled_for)'))


if __name__=='__main__':migrate()
