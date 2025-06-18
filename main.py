import time
import tracemalloc

import duckdb
import pandas as pd
import polars as pl
from memory_profiler import memory_usage


# Configurações
TEST_REPEATS = 3
DATASET_PATH_CSV = "microdados_enem_2023/DADOS/MICRODADOS_ENEM_2023.csv"
DATASET_PATH_PARQUET = "microdados_enem_2023/DADOS/MICRODADOS_ENEM_2023_pequeno.parquet"
ITENS_PATH_CSV = "microdados_enem_2023/DADOS/ITENS_PROVA_2023.csv"

results = {
    "engine": [],
    "operation": [],
    "scenario": [],
    "time_seconds": [],
    "memory_mb": [],
}


def benchmark(func, *args, scenario="pequeno", engine="engine", operation="op"):
    for _ in range(TEST_REPEATS):
        tracemalloc.start()
        start_mem = memory_usage()[0]
        start_time = time.time()

        try:
            func(*args)
        except Exception as e:
            print(f"Erro em {engine} - {operation}: {e}")
            tracemalloc.stop()
            continue

        exec_time = time.time() - start_time
        peak_mem = max(memory_usage()) - start_mem
        tracemalloc.stop()

        results["engine"].append(engine)
        results["operation"].append(operation)
        results["scenario"].append(scenario)
        results["time_seconds"].append(exec_time)
        results["memory_mb"].append(peak_mem)


# === DUCKDB ===
def duckdb_read_csv(path):
    duckdb.query(
        f"SELECT * FROM read_csv_auto('{path}', delim=';', encoding='latin-1')"
    )


def duckdb_read_parquet(path):
    duckdb.query(f"SELECT * FROM read_parquet('{DATASET_PATH_PARQUET}')")


def duckdb_filter(path):
    duckdb.query(f"""
        SELECT * 
        FROM read_csv_auto('{path}', delim=';', encoding='latin-1') 
        WHERE NU_NOTA_MT > 600
    """)


def duckdb_join(path):
    duckdb.query(f"""
        SELECT 
            *
        FROM 
            (SELECT * FROM read_csv_auto('{path}', sep=';', encoding='latin-1')) AS dados
        JOIN 
            (SELECT DISTINCT CO_PROVA, SG_AREA FROM read_csv_auto('{ITENS_PATH_CSV}', sep=';', encoding='latin-1')) AS itens
        ON 
            dados.CO_PROVA_MT = itens.CO_PROVA
    """)


def duckdb_agg(path):
    duckdb.query(f"""
        SELECT CO_UF_ESC, AVG(NU_NOTA_MT)
        FROM read_csv_auto('{path}', delim=';', encoding='latin-1')
        GROUP BY CO_UF_ESC
    """)


def duckdb_write_csv(path):
    duckdb.query(f"""
        COPY (SELECT * FROM read_csv_auto('{path}', delim=';', encoding='latin-1'))
        TO 'output.csv' (FORMAT CSV)
    """)


def duckdb_write_parquet(path):
    duckdb.query(f"""
        COPY (SELECT * FROM read_csv_auto('{path}', delim=';', encoding='latin-1'))
        TO 'output.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)


# === PANDAS ===
def pandas_read_csv(path):
    pd.read_csv(path, sep=";", encoding="latin-1")


def pandas_read_parquet(path):
    pd.read_parquet(DATASET_PATH_PARQUET, engine="pyarrow")


def pandas_filter(path):
    pd.read_csv(path, sep=";", encoding="latin-1").query("NU_NOTA_MT > 600")


def pandas_join(path):
    dados = pd.read_csv(
        path, sep=";", encoding="latin-1"
    )
    itens = pd.read_csv(
        ITENS_PATH_CSV,
        sep=";",
        encoding="latin-1",
        usecols=["CO_PROVA", "SG_AREA"],
    ).drop_duplicates()
    dados.merge(itens, left_on="CO_PROVA_MT", right_on="CO_PROVA")


def pandas_agg(path):
    pd.read_csv(path, sep=";", encoding="latin-1").groupby("CO_UF_ESC")[
        "NU_NOTA_MT"
    ].mean()


def pandas_write_csv(path):
    df = pd.read_csv(path, sep=";", encoding="latin-1")
    df.to_csv("output.csv", index=False)


def pandas_write_parquet(path):
    df = pd.read_csv(path, sep=";", encoding="latin-1")
    df.to_parquet("output.parquet", compression="snappy", index=False)


# === POLARS ===
def polars_read_csv(path):
    pl.read_csv(path, separator=";", encoding="latin-1")


def polars_read_parquet(path):
    pl.read_parquet(DATASET_PATH_PARQUET, use_pyarrow=True)


def polars_filter(path):
    pl.read_csv(path, separator=";", encoding="latin-1").filter(
        pl.col("NU_NOTA_MT") > 600
    )


def polars_join(path):
    dados = pl.read_csv(
        path, separator=";", encoding="latin-1"
    )
    itens = pl.read_csv(
        ITENS_PATH_CSV,
        separator=";",
        encoding="latin-1",
        columns=["CO_PROVA", "SG_AREA"],
    ).unique()
    return dados.join(itens, left_on="CO_PROVA_MT", right_on="CO_PROVA")


def polars_agg(path):
    pl.read_csv(path, separator=";", encoding="latin-1").group_by("CO_UF_ESC").agg(
        pl.col("NU_NOTA_MT").mean()
    )


def polars_write_csv(path):
    df = pl.read_csv(path, separator=";", encoding="latin-1")
    df.write_csv("output.csv")
    return df


def polars_write_parquet(path):
    df = pl.read_csv(path, separator=";", encoding="latin-1")
    df.write_parquet("output.parquet", compression="snappy")


# === EXECUÇÃO ===
for engine, funcs in [
    (
        "duckdb",
        [
            duckdb_read_csv,
            duckdb_read_parquet,
            duckdb_filter,
            duckdb_join,
            duckdb_agg,
            duckdb_write_csv,
            duckdb_write_parquet,
        ],
    ),
    (
        "pandas",
        [
            pandas_read_csv,
            pandas_read_parquet,
            pandas_filter,
            pandas_join,
            pandas_agg,
            pandas_write_csv,
            pandas_write_parquet,
        ],
    ),
    (
        "polars",
        [
            polars_read_csv,
            polars_read_parquet,
            polars_filter,
            polars_join,
            polars_agg,
            polars_write_csv,
            polars_write_parquet,
        ],
    ),
]:
    print(f"Benchmarking {engine}...")
    for op_func, op_name in zip(funcs, ["read_csv", "read_parquet", "filter", "join", "agg", "write_csv", "write_parquet"]):
        if engine == "pandas" and op_name == "write_csv":
            continue
        print(f"  Executando operação: {op_name}")
        benchmark(
            op_func,
            DATASET_PATH_CSV,
            scenario="grande",
            engine=engine,
            operation=op_name,
        )

# Exportar
df_results = pd.DataFrame(results)
df_results.to_csv("benchmark_resultados_pequeno.csv", index=False)
print(
    df_results.groupby(["engine", "operation"]).agg(
        {"time_seconds": "mean", "memory_mb": "mean"}
    )
)