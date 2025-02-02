import os
from glob import glob

import pandas as pd
import plotly.graph_objects as go  # type: ignore


def extract_params(file_name: str) -> tuple[str, str]:
    """Extract executor and function names from benchmark filename."""
    base_name = os.path.basename(file_name)
    if base_name.startswith("duckdb"):
        executor = "DuckDB"
        function = base_name.replace("duckdb_", "")
    elif base_name.startswith("pure_pa"):
        executor = "PyArrow"
        function = base_name.replace("pure_pa_", "")
    else:
        raise ValueError(f"Unknown executor: {base_name}")

    return executor, function


def read_mprof_file(file_path: str) -> pd.DataFrame:
    """Read mprof output file and return DataFrame with memory usage."""
    df = pd.read_csv(
        file_path, skiprows=1, names=["X", "memory_mb", "timestamp"], delimiter=" "
    )
    executor, function = extract_params(file_path)
    df["timestamp"] = df["timestamp"] - df["timestamp"].min()
    df["executor"] = executor
    df["function"] = function
    return df


def create_plots():
    # Create output directory
    os.makedirs("images", exist_ok=True)

    # Read all benchmark files
    dfs = []
    for file_path in glob("benchmark_results/*"):
        df = read_mprof_file(file_path)
        dfs.append(df)

    # Combine all data
    all_data = pd.concat(dfs)

    print(all_data.head())

    # Create one plot per function
    for function in all_data["function"].unique():
        function_data = all_data[all_data["function"] == function]
        function_data = function_data.sort_values(by=["executor", "timestamp"])

        fig = go.Figure()

        for executor in function_data["executor"].unique():
            executor_data = function_data[function_data["executor"] == executor]

            fig.add_trace(
                go.Scatter(
                    x=executor_data["timestamp"],
                    y=executor_data["memory_mb"],
                    name=executor,
                    mode="lines",
                )
            )

        fig.update_layout(
            title=f"Memory Usage: {function}",
            xaxis_title="Time (s)",
            yaxis_title="Memory (MB)",
            legend_title="Executor",
        )

        # Save plot as PNG
        fig.write_image(f"images/{function}.jpg", engine="auto")


if __name__ == "__main__":
    create_plots()
