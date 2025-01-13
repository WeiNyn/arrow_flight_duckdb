# Arrow Flight Polars

A project to read Parquet data from an Apache Arrow Flight server using Polars.

## Description

This project demonstrates how to use Apache Arrow Flight to retrieve Parquet data and process it using the Polars library in Python.

## Installation

To install the required dependencies, run:

```sh
pip install numpy pandas polars pyarrow
```

## Usage

Here is an example of how to use the `FlightParquetReader` class to read data from an Arrow Flight server and process it with Polars:

```python
import tracemalloc
import logging
import polars as pl
import pyarrow.flight as flight
from client import FlightParquetReader

logging.basicConfig(level=logging.INFO)

def main():
    tracemalloc.start()  # Start tracing memory allocations
    client = flight.FlightClient("grpc://localhost:8815")
    ticket = flight.Ticket(b"")
    reader = FlightParquetReader(client, ticket)
    current, peak = tracemalloc.get_traced_memory()
    logging.info(
        f"Current memory usage: {current / 10**6}MB; Peak: {peak / 10**6}MB"
    )  # Debug line

    # Use the FlightParquetReader as a file-like object for scan_parquet
    df = pl.scan_parquet(reader.temp_file_path)
    logging.info(df)

    df = df.select(pl.col("col_0"), pl.col("col_1")).collect()
    logging.info(df)

    current, peak = tracemalloc.get_traced_memory()
    logging.info(
        f"Current memory usage: {current / 10**6}MB; Peak: {peak / 10**6}MB"
    )  # Debug line
    tracemalloc.stop()  # Stop tracing memory allocations
    reader.close()

if __name__ == "__main__":
    main()
```

## License

This project is licensed under the MIT License.
