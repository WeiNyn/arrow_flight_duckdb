from abc import ABC, abstractmethod

import pyarrow
import pyarrow.compute
from pyarrow.flight import FlightClient, Ticket

from arrow_flight_duckdb.duckdb_client import DuckdbClient


class BaseExecutor(ABC):
    def __init__(self, flight_client: FlightClient):
        self.flight_client = flight_client

    @abstractmethod
    def select_start(self) -> pyarrow.Table:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def select_start_limit(self, limit: int) -> pyarrow.Table:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def groupby_count(self) -> pyarrow.Table:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def groupby_sum(self) -> pyarrow.Table:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def groupby_avg(self) -> pyarrow.Table:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def groupby_min_max(self) -> pyarrow.Table:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def groupby_complex(self) -> pyarrow.Table:
        raise NotImplementedError("Method not implemented")


class DuckdbExecutor(BaseExecutor):
    def __init__(self, flight_client: FlightClient):
        super().__init__(flight_client)
        self.duckdb_client = DuckdbClient()

    def select_start(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("select_start"))

        self.duckdb_client.register_reader(flight_stream.to_reader(), "select_start")
        table = self.duckdb_client.execute("SELECT * FROM select_start")
        return table.to_arrow_table()

    def select_start_limit(self, limit: int) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("select_start_limit"))

        self.duckdb_client.register_reader(
            flight_stream.to_reader(), "select_start_limit"
        )
        table = self.duckdb_client.execute(
            f"SELECT * FROM select_start_limit LIMIT {limit}"
        )
        return table.to_arrow_table()

    def groupby_count(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_count"))

        self.duckdb_client.register_reader(flight_stream.to_reader(), "groupby_count")
        table = self.duckdb_client.execute(
            "SELECT col_1, count(*) FROM groupby_count GROUP BY col_1"
        )
        return table.to_arrow_table()

    def groupby_sum(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_sum"))

        self.duckdb_client.register_reader(flight_stream.to_reader(), "groupby_sum")
        table = self.duckdb_client.execute(
            "SELECT col_1, sum(col_2) FROM groupby_sum GROUP BY col_1"
        )
        return table.to_arrow_table()

    def groupby_avg(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_avg"))

        self.duckdb_client.register_reader(flight_stream.to_reader(), "groupby_avg")
        table = self.duckdb_client.execute(
            "SELECT col_1, avg(col_2) FROM groupby_avg GROUP BY col_1"
        )
        return table.to_arrow_table()

    def groupby_min_max(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_min_max"))

        self.duckdb_client.register_reader(flight_stream.to_reader(), "groupby_min_max")
        table = self.duckdb_client.execute(
            "SELECT col_1, min(col_2), max(col_2) FROM groupby_min_max GROUP BY col_1"
        )
        return table.to_arrow_table()

    def groupby_complex(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_complex"))

        self.duckdb_client.register_reader(flight_stream.to_reader(), "groupby_complex")
        table = self.duckdb_client.execute(
            """SELECT 
                        col_1, col_2, count(*), sum(col_3), avg(col_3), min(col_3), max(col_3) 
                    FROM 
                        groupby_complex 
                    GROUP 
                        BY col_1, col_2"""
        )
        return table.to_arrow_table()


class PurePAExecutor(BaseExecutor):
    def select_start(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("select_start"))
        return flight_stream.read_all()

    def select_start_limit(self, limit: int) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("select_start_limit"))
        data = flight_stream.read_all()
        data = data.take(list(range(limit)))
        return data

    def groupby_count(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_count"))
        data = flight_stream.read_all()
        data = data.group_by("col_1").aggregate([("col_1", "count")])
        return data

    def groupby_sum(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_sum"))
        data = flight_stream.read_all()
        data = data.group_by("col_1").aggregate([("col_2", "sum")])
        return data

    def groupby_avg(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_avg"))
        data = flight_stream.read_all()
        data = data.group_by("col_1").aggregate([("col_2", "mean")])
        return data

    def groupby_min_max(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_min_max"))
        data = flight_stream.read_all()
        data = data.group_by("col_1").aggregate([("col_2", "min"), ("col_2", "max")])
        return data

    def groupby_complex(self) -> pyarrow.Table:
        flight_stream = self.flight_client.do_get(ticket=Ticket("groupby_complex"))
        data = flight_stream.read_all()
        data = data.group_by(["col_1", "col_2"]).aggregate(
            [
                ("col_3", "count"),
                ("col_3", "sum"),
                ("col_3", "mean"),
                ("col_3", "min"),
                ("col_3", "max"),
            ]
        )
        return data


if __name__ == "__main__":
    import sys

    target = sys.argv[1]
    benchmark_function = sys.argv[2]

    client = FlightClient.connect("grpc://localhost:8815")

    benchmark_executor = (
        DuckdbExecutor(client) if target == "duckdb" else PurePAExecutor(client)
    )

    if benchmark_function == "select_start":
        benchmark_executor.select_start()
    elif benchmark_function == "select_start_limit":
        benchmark_executor.select_start_limit(100)
    elif benchmark_function == "groupby_count":
        benchmark_executor.groupby_count()
    elif benchmark_function == "groupby_sum":
        benchmark_executor.groupby_sum()
    elif benchmark_function == "groupby_avg":
        benchmark_executor.groupby_avg()
    elif benchmark_function == "groupby_min_max":
        benchmark_executor.groupby_min_max()
    elif benchmark_function == "groupby_complex":
        benchmark_executor.groupby_complex()
