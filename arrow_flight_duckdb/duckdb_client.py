"""DuckDB client for interacting with Arrow RecordBatchReader.

This module provides a client for DuckDB operations, particularly focused on
working with Apache Arrow RecordBatchReader objects and SQL operations.

Example:
    >>> import pyarrow as pa
    >>> from arrow_flight_duckdb.duckdb_client import DuckdbClient
    >>>
    >>> # Create sample data
    >>> data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    >>> table = pa.Table.from_pydict(data)
    >>> reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    >>>
    >>> # Initialize client and register data
    >>> client = DuckdbClient()
    >>> client.register_reader(reader, "my_view")
    >>>
    >>> # Execute query
    >>> result = client.execute("SELECT * FROM my_view")
    >>> print(result.fetchall())
    [(1, 'a'), (2, 'b'), (3, 'c')]
"""

import os

import duckdb
from pyarrow import RecordBatchReader

os.environ["ACERO_ALIGNMENT_HANDLING"] = "ignore"


class DuckdbClient:
    """A client for interacting with DuckDB using Arrow RecordBatchReader.

    Attributes:
        persist_path (str): Path to DuckDB database file or ":memory:" for in-memory database
        conn (duckdb.DuckDBPyConnection): DuckDB connection object
    """

    def __init__(self, session_name: str | None = None) -> None:
        self.persist_path = (
            ":memory:" if session_name is None else f"/tmp/{session_name}.duckdb"
        )
        self.conn = duckdb.connect(":memory:")

    def is_view_exists(self, view_name: str) -> bool:
        """Check if a view exists in the DuckDB connection.

        Args:
            view_name: Name of the view to check

        Returns:
            bool: True if view exists, False otherwise

        Example:
            >>> client = DuckdbClient()
            >>> client.is_view_exists("my_view")
            False
        """
        try:
            self.conn.view(view_name=view_name)
            return True
        except duckdb.CatalogException:
            return False

    def register_reader(
        self, reader: RecordBatchReader, view_name: str, overwrite: bool = False
    ) -> None:
        """Register an Arrow RecordBatchReader as a view in DuckDB.

        Args:
            reader: Arrow RecordBatchReader object to register
            view_name: Name for the registered view
            overwrite: If True, overwrites existing view with same name

        Raises:
            ValueError: If view exists and overwrite is False

        Example:
            >>> data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
            >>> table = pa.Table.from_pydict(data)
            >>> reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
            >>> client.register_reader(reader, "my_view")
        """
        if self.is_view_exists(view_name):
            if overwrite:
                self.conn.execute(f"DROP VIEW {view_name}")
            else:
                raise ValueError(f"View {view_name} already exists")

        self.conn.register(view_name=view_name, python_object=reader)

    def execute(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query on the DuckDB connection.

        Args:
            query: SQL query string to execute

        Returns:
            DuckDBPyRelation: Query result relation

        Example:
            >>> client.execute("SELECT * FROM my_view LIMIT 2").fetchall()
            [(1, 'a'), (2, 'b')]
        """
        return self.conn.query(query)

    def persist(self, query: str, table_name: str) -> None:
        """Persist query results as a table in DuckDB.

        Args:
            query: SQL query to execute and persist
            table_name: Name for the created table

        Example:
            >>> client.persist("SELECT * FROM my_view", "permanent_table")
            >>> client.execute("SELECT * FROM permanent_table").fetchall()
            [(1, 'a'), (2, 'b'), (3, 'c')]
        """
        self.conn.execute(f"CREATE TABLE {table_name} AS {query}")
