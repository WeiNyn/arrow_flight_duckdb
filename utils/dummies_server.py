import random
import signal
import threading

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import flight


class RandomTableFlightServer(flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self._tables = {}

    def do_get(self, _context, _ticket):
        table = self._generate_random_table()
        batches = table.to_batches(max_chunksize=500)  # Chunk size of 100 rows
        return flight.GeneratorStream(table.schema, self._batch_generator(batches))

    def _batch_generator(self, batches):
        yield from batches

    def _generate_random_table(self):
        num_rows = 1_000_000  # Set to 1 million rows
        num_cols = random.randint(4, 4)
        data ={f"col_{i}": np.random.rand(num_rows) for i in range(num_cols)}
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)
        return table


def main():
    server = RandomTableFlightServer("grpc://localhost:8815")
    server_thread = threading.Thread(target=server.serve)
    server_thread.start()
    print("Server started on grpc://localhost:8815")

    def shutdown(_signum, _frame):
        print("Shutting down server...")
        server.shutdown()
        server_thread.join()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    server_thread.join()


if __name__ == "__main__":
    main()
