import os
import queue
import tempfile
import threading
import tracemalloc
import logging

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.flight as flight

logging.basicConfig(level=logging.INFO)


class FlightParquetReader:
    """
    A class to read Parquet data from an Apache Arrow Flight server and write it to a temporary file.

    Attributes:
        client (flight.FlightClient): The Flight client to use for data retrieval.
        ticket (flight.Ticket): The ticket to use for the Flight request.
        queue (queue.Queue): A queue to store data batches.
        stop_event (threading.Event): An event to signal stopping of data retrieval.
        total_rows (int): The total number of rows retrieved.
        reader (flight.FlightStreamReader): The Flight stream reader.
        temp_file_path (str): The path to the temporary file where data is written.
    """

    def __init__(self, client, ticket):
        """
        Initialize the FlightParquetReader with a Flight client and ticket.

        Args:
            client (flight.FlightClient): The Flight client to use for data retrieval.
            ticket (flight.Ticket): The ticket to use for the Flight request.
        """
        self.client = client
        self.ticket = ticket
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        self.total_rows = 0
        self.reader = None
        self.temp_file_path = tempfile.mktemp(suffix=".parquet")
        self._write_stream_to_file()

    def _write_stream_to_file(self):
        """
        Write the data stream from the Flight server to a temporary Parquet file.
        """
        try:
            self.reader = self.client.do_get(self.ticket)
            with open(self.temp_file_path, "wb") as temp_file:
                writer = None
                for batch in self.reader:
                    table = pa.Table.from_batches([batch.data])
                    if writer is None:
                        writer = pq.ParquetWriter(temp_file, table.schema)
                    writer.write_table(table)
                if writer:
                    writer.close()
            logging.info("Data written to temporary file: %s", self.temp_file_path)
        except Exception as e:
            logging.error("Error writing stream to file: %s", e)
            self.cleanup()

    def cleanup(self):
        """
        Clean up resources by stopping the data retrieval and removing the temporary file.
        """
        self.stop_event.set()
        if os.path.exists(self.temp_file_path):
            os.remove(self.temp_file_path)
            logging.info("Temporary file removed: %s", self.temp_file_path)

    def close(self):
        """
        Close the FlightParquetReader and clean up resources.
        """
        self.cleanup()


def main():
    """
    Main function to demonstrate the usage of FlightParquetReader and log memory usage.
    """
    import time

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
