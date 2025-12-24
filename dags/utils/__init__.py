"""
Módulo de utilitários compartilhados para DAGs.
"""

from .bronze_helpers import (
    ingest_csv_to_bronze,
    validate_file_exists,
    read_csv_with_validation,
    create_bronze_table,
    get_duckdb_connection,
    BronzeIngestionError,
    DB_PATH
)

from .silver_helpers import (
    transform_bronze_to_silver,
    test_silver_query,
    create_silver_table,
    SilverTransformationError
)

from .gold_helpers import (
    verify_silver_tables_exist,
    create_gold_table,
    GoldTransformationError
)

__all__ = [
    # Bronze
    'ingest_csv_to_bronze',
    'validate_file_exists',
    'read_csv_with_validation',
    'create_bronze_table',
    'BronzeIngestionError',
    # Silver
    'transform_bronze_to_silver',
    'test_silver_query',
    'create_silver_table',
    'SilverTransformationError',
    # Gold
    'verify_silver_tables_exist',
    'create_gold_table',
    'GoldTransformationError',
    # Common
    'get_duckdb_connection',
    'DB_PATH'
]

