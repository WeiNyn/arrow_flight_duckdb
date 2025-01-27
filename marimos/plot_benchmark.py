import marimo

__generated_with = "0.10.17"
app = marimo.App(width="medium")


@app.cell
def _():
    return


@app.cell
def _():
    from glob import glob

    import pandas as pd
    from plotly import express as px
    from plotly import graph_objects as go
    from plotly.subplots import make_subplots
    return glob, go, make_subplots, pd, px


@app.cell
def _(glob):
    benchmark_files = glob("benchmark_results/*")
    benchmark_files
    return (benchmark_files,)


@app.cell
def _(benchmark_files):
    def extract_param(file_name: str) -> tuple[str, str]:
        file_name = file_name.split("/")[1]

        if "duckdb" in file_name:
            return "duckdb", file_name.replace("duckdb_", "")
        else:
            return "pure_pa", file_name.replace("pure_pa_", "")

    benchmark_suites = [extract_param(f) for f in benchmark_files]
    benchmark_suites
    return benchmark_suites, extract_param


@app.cell
def _(benchmark_files, pd):
    def read_result(file: str) -> pd.DataFrame:
        df = pd.read_csv(file, skiprows=1, header=None, delimiter=" ")
        df.columns = ["X", "MEM", "TS"]
        df.drop(columns="X", inplace=True)
        df["SUIT"] = file
        df["INDEX"] = df["TS"] - df["TS"].min()
        df["INDEX"] = df["INDEX"].apply(lambda x: int(x * 1000))

        return df

    all_dfs = []
    for f in benchmark_files:
        df = read_result(f)
        all_dfs.append(df)

    all_df = pd.concat(all_dfs)
    all_df 
    return all_df, all_dfs, df, f, read_result


@app.cell
def _(benchmark_suites):
    benchmark_functions = list(set([s[1] for s in benchmark_suites]))
    benchmark_functions
    return (benchmark_functions,)


@app.cell
def _(all_df, benchmark_functions, go, make_subplots):
    fig = make_subplots(rows=len(benchmark_functions), cols=1, subplot_titles=benchmark_functions)
    fig.update_layout(
        width=1024,
        height=2048
    )

    for row, function in enumerate(benchmark_functions):
        sub_df = all_df[all_df["SUIT"] == f"benchmark_results/duckdb_{function}"]
        fig.add_trace(go.Scatter(x=sub_df["INDEX"], y=sub_df["MEM"], name="duckdb"), row=row + 1, col=1)

        sub_df = all_df[all_df["SUIT"] == f"benchmark_results/pure_pa_{function}"]
        fig.add_trace(go.Scatter(x=sub_df["INDEX"], y=sub_df["MEM"], name="pure_pa"), row=row + 1, col=1)

    fig.show()
    return fig, function, row, sub_df


if __name__ == "__main__":
    app.run()
