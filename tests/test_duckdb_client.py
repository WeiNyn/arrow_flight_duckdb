import pyarrow as pa
import pytest

from arrow_flight_duckdb.duckdb_client import DuckdbClient


@pytest.fixture
def duckdb_client():
    return DuckdbClient()


@pytest.fixture
def sample_batch_reader():
    # Create sample data
    data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    table = pa.Table.from_pydict(data)
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


def test_init_default():
    client = DuckdbClient()
    assert client.persist_path == ":memory:"


def test_init_with_session():
    client = DuckdbClient("test_session")
    assert client.persist_path == "/tmp/test_session.duckdb"


def test_is_view_exists(duckdb_client, sample_batch_reader):
    view_name = "test_view"
    assert not duckdb_client.is_view_exists(view_name)
    duckdb_client.register_reader(sample_batch_reader, view_name)
    assert duckdb_client.is_view_exists(view_name)


def test_register_reader(duckdb_client, sample_batch_reader):
    view_name = "test_view"
    duckdb_client.register_reader(sample_batch_reader, view_name)
    result = duckdb_client.execute(f"SELECT * FROM {view_name}").fetchall()
    assert len(result) == 3


def test_register_reader_overwrite(duckdb_client, sample_batch_reader):
    view_name = "test_view"
    duckdb_client.register_reader(sample_batch_reader, view_name)
    duckdb_client.register_reader(sample_batch_reader, view_name, overwrite=True)
    result = duckdb_client.execute(f"SELECT * FROM {view_name}").fetchall()
    assert len(result) == 3


def test_register_reader_duplicate_error(duckdb_client, sample_batch_reader):
    view_name = "test_view"
    duckdb_client.register_reader(sample_batch_reader, view_name)
    with pytest.raises(ValueError, match=f"View {view_name} already exists"):
        duckdb_client.register_reader(sample_batch_reader, view_name)


def test_execute(duckdb_client, sample_batch_reader):
    view_name = "test_view"
    duckdb_client.register_reader(sample_batch_reader, view_name)
    result = duckdb_client.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
    assert result == 3


def test_persist(duckdb_client, sample_batch_reader):
    view_name = "test_view"
    table_name = "test_table"
    duckdb_client.register_reader(sample_batch_reader, view_name)
    duckdb_client.persist(f"SELECT * FROM {view_name}", table_name)
    result = duckdb_client.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    assert result == 3
