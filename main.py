import gc
import os
import sys
import time

import duckdb
import pandas as pd
import polars as pl
import psutil

# Configurações
TEST_REPEATS = 10
ITENS_PATH_CSV = "microdados_enem_2023/DADOS/ITENS_PROVA_2023.csv"


def get_dataset_paths(scenario):
    """Retorna os caminhos dos arquivos CSV e Parquet baseado no cenário"""
    base_path = "microdados_enem_2023/DADOS/MICRODADOS_ENEM_2023"

    if scenario == "grande":
        csv_path = f"{base_path}.csv"
        parquet_path = f"{base_path}.parquet"
        encoding = "latin-1"
    else:
        csv_path = f"{base_path}_{scenario}.csv"
        parquet_path = f"{base_path}_{scenario}.parquet"
        encoding = "utf-8"

    return csv_path, parquet_path, encoding


results = {
    "engine": [],
    "operation": [],
    "scenario": [],
    "time_seconds": [],
    "memory_mb": [],
}


def get_memory_usage():
    """Retorna o uso de memória atual do processo em MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def benchmark(func, *args, scenario="pequeno", engine="engine", operation="op"):
    for _ in range(TEST_REPEATS):
        # Limpar garbage collector antes de medir
        for _ in range(3):  # Múltiplas coletas para garantir limpeza
            gc.collect()

        # Aguardar um pouco para estabilizar a memória
        time.sleep(0.1)

        # Medir memória inicial (média de 3 medições)
        mem_readings = []
        for _ in range(3):
            mem_readings.append(get_memory_usage())
            time.sleep(0.01)
        start_mem = min(mem_readings)  # Usar o menor valor

        start_time = time.time()

        try:
            result = func(*args)
            # Forçar que o resultado seja usado para evitar lazy evaluation
            if hasattr(result, "collect"):
                result.collect()
        except Exception as e:
            print(f"Erro em {engine} - {operation}: {e}")
            continue

        exec_time = time.time() - start_time

        # Medir memória após execução (pico)
        end_mem = get_memory_usage()

        # Calcular diferença de memória
        memory_diff = end_mem - start_mem

        # Se a diferença for negativa (improvável mas possível),
        # usar um valor pequeno positivo
        peak_mem = max(0.1, memory_diff)

        # Limpar memória após medição
        if "result" in locals():
            del result
        for _ in range(3):
            gc.collect()

        results["engine"].append(engine)
        results["operation"].append(operation)
        results["scenario"].append(scenario)
        results["time_seconds"].append(exec_time)
        results["memory_mb"].append(peak_mem)


# === DUCKDB ===
def duckdb_read_csv(path, encoding="utf-8"):
    enc_param = f", encoding='{encoding}'" if encoding == "latin-1" else ""
    duckdb.query(f"SELECT * FROM read_csv_auto('{path}', delim=';'{enc_param})")


def duckdb_read_parquet(parquet_path, encoding="utf-8"):
    duckdb.query(f"SELECT * FROM read_parquet('{parquet_path}')")


def duckdb_filter(path, encoding="utf-8"):
    enc_param = f", encoding='{encoding}'" if encoding == "latin-1" else ""
    duckdb.query(f"""
        SELECT *
        FROM read_csv_auto('{path}', delim=';'{enc_param})
        WHERE NU_NOTA_MT > 600
    """)


def duckdb_join(path, encoding="utf-8"):
    enc_param = f", encoding='{encoding}'" if encoding == "latin-1" else ""
    duckdb.query(f"""
        SELECT
            *
        FROM
            (SELECT * FROM read_csv_auto('{path}', sep=';'{enc_param})) AS dados
        JOIN
            (SELECT DISTINCT CO_PROVA, SG_AREA FROM read_csv_auto('{ITENS_PATH_CSV}', sep=';', encoding='latin-1')) AS itens
        ON
            dados.CO_PROVA_MT = itens.CO_PROVA
    """)


def duckdb_agg(path, encoding="utf-8"):
    enc_param = f", encoding='{encoding}'" if encoding == "latin-1" else ""
    duckdb.query(f"""
        SELECT CO_UF_ESC, AVG(NU_NOTA_MT)
        FROM read_csv_auto('{path}', delim=';'{enc_param})
        GROUP BY CO_UF_ESC
    """)


def duckdb_write_csv(path, encoding="utf-8"):
    enc_param = f", encoding='{encoding}'" if encoding == "latin-1" else ""
    duckdb.query(f"""
        COPY (SELECT * FROM read_csv_auto('{path}', delim=';'{enc_param}))
        TO 'output.csv' (FORMAT CSV)
    """)


def duckdb_write_parquet(path, encoding="utf-8"):
    enc_param = f", encoding='{encoding}'" if encoding == "latin-1" else ""
    duckdb.query(f"""
        COPY (SELECT * FROM read_csv_auto('{path}', delim=';'{enc_param}))
        TO 'output.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)


# === PANDAS ===
def pandas_read_csv(path, encoding="utf-8"):
    pd.read_csv(path, sep=";", encoding=encoding)


def pandas_read_parquet(parquet_path, encoding="utf-8"):
    pd.read_parquet(parquet_path, engine="pyarrow")


def pandas_filter(path, encoding="utf-8"):
    pd.read_csv(path, sep=";", encoding=encoding).query("NU_NOTA_MT > 600")


def pandas_join(path, encoding="utf-8"):
    dados = pd.read_csv(path, sep=";", encoding=encoding)
    itens = pd.read_csv(
        ITENS_PATH_CSV,
        sep=";",
        encoding="latin-1",
        usecols=["CO_PROVA", "SG_AREA"],
    ).drop_duplicates()
    dados.merge(itens, left_on="CO_PROVA_MT", right_on="CO_PROVA")


def pandas_agg(path, encoding="utf-8"):
    pd.read_csv(path, sep=";", encoding=encoding).groupby("CO_UF_ESC")["NU_NOTA_MT"].mean()


def pandas_write_csv(path, encoding="utf-8"):
    df = pd.read_csv(path, sep=";", encoding=encoding)
    df.to_csv("output.csv", index=False)


def pandas_write_parquet(path, encoding="utf-8"):
    df = pd.read_csv(path, sep=";", encoding=encoding)
    df.to_parquet("output.parquet", compression="snappy", index=False)


# === POLARS ===
def polars_read_csv(path, encoding="utf-8"):
    pl.read_csv(path, separator=";", encoding=encoding)


def polars_read_parquet(parquet_path, encoding="utf-8"):
    pl.read_parquet(parquet_path, use_pyarrow=True)


def polars_filter(path, encoding="utf-8"):
    pl.read_csv(path, separator=";", encoding=encoding).filter(pl.col("NU_NOTA_MT") > 600)


def polars_join(path, encoding="utf-8"):
    dados = pl.read_csv(path, separator=";", encoding=encoding)
    itens = (
        pl.read_csv(
            ITENS_PATH_CSV,
            separator=";",
            encoding="latin-1",
            columns=["CO_PROVA", "SG_AREA"],
        )
        # .with_columns(pl.col("CO_PROVA").cast(pl.Int64))
        .unique()
    )
    return dados.join(itens, left_on="CO_PROVA_MT", right_on="CO_PROVA")


def polars_agg(path, encoding="utf-8"):
    pl.read_csv(path, separator=";", encoding=encoding).group_by("CO_UF_ESC").agg(
        pl.col("NU_NOTA_MT").mean()
    )


def polars_write_csv(path, encoding="utf-8"):
    df = pl.read_csv(path, separator=";", encoding=encoding)
    df.write_csv("output.csv")
    return df


def polars_write_parquet(path, encoding="utf-8"):
    df = pl.read_csv(path, separator=";", encoding=encoding)
    df.write_parquet("output.parquet", compression="snappy")


# === EXECUÇÃO ===
# Definir o cenário desejado (pode ser: "pequeno", "medio", "grande")
# Pode ser passado como argumento da linha de comando
if len(sys.argv) > 1:
    SCENARIO = sys.argv[1]
    if SCENARIO not in ["pequeno", "medio", "grande"]:
        print("Erro: Cenário deve ser 'pequeno', 'medio' ou 'grande'")
        print("Uso: python main.py [pequeno|medio|grande]")
        sys.exit(1)
else:
    SCENARIO = "pequeno"  # Valor padrão

print(f"Executando benchmark para o cenário: {SCENARIO}")
csv_path, parquet_path, encoding = get_dataset_paths(SCENARIO)
print(f"Arquivo CSV: {csv_path}")
print(f"Arquivo Parquet: {parquet_path}")
print(f"Encoding: {encoding}")

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
]:
    print(f"Benchmarking {engine}...")
    for op_func, op_name in zip(
        funcs,
        [
            "read_csv",
            "read_parquet",
            "filter",
            "join",
            "agg",
            "write_csv",
            "write_parquet",
        ],
    ):
        print(f"  Executando operação: {op_name}")

        # Usar o caminho correto baseado na operação
        if op_name == "read_parquet":
            benchmark(
                op_func,
                parquet_path,
                encoding,
                scenario=SCENARIO,
                engine=engine,
                operation=op_name,
            )
        else:
            benchmark(
                op_func,
                csv_path,
                encoding,
                scenario=SCENARIO,
                engine=engine,
                operation=op_name,
            )

# Exportar com nome baseado no cenário
df_results = pd.DataFrame(results)
output_filename = f"benchmark_resultados_{SCENARIO}.csv"
df_results.to_csv(output_filename, index=False)
print(f"Resultados salvos em: {output_filename}")
print(
    df_results.groupby(["engine", "operation"]).agg({"time_seconds": "mean", "memory_mb": "mean"})
)


# === CONSOLIDAÇÃO DOS RESULTADOS ===
def consolidar_resultados():
    """Consolida todos os arquivos de benchmark em um arquivo geral"""

    # Buscar todos os arquivos de benchmark existentes
    arquivos_benchmark = []
    cenarios_possiveis = ["pequeno", "medio", "grande"]

    for cenario in cenarios_possiveis:
        arquivo = f"benchmark_resultados_{cenario}.csv"
        if os.path.exists(arquivo):
            arquivos_benchmark.append(arquivo)
            print(f"Encontrado: {arquivo}")

    if not arquivos_benchmark:
        print("Nenhum arquivo de benchmark encontrado para consolidação")
        return

    # Ler e consolidar todos os arquivos
    df_consolidado = pd.DataFrame()

    for arquivo in arquivos_benchmark:
        try:
            df_temp = pd.read_csv(arquivo)
            df_consolidado = pd.concat([df_consolidado, df_temp], ignore_index=True)
            print(f"Adicionado {len(df_temp)} registros de {arquivo}")
        except Exception as e:
            print(f"Erro ao ler {arquivo}: {e}")

    if not df_consolidado.empty:
        # Salvar arquivo consolidado
        arquivo_consolidado = "resultados_geral.csv"
        df_consolidado.to_csv(arquivo_consolidado, index=False)
        print(f"\n✅ Arquivo consolidado criado: {arquivo_consolidado}")
        print(f"Total de registros: {len(df_consolidado)}")

        # Mostrar resumo por cenário
        print("\n📊 Resumo por cenário:")
        resumo = (
            df_consolidado.groupby(["scenario", "engine", "operation"])
            .agg({"time_seconds": ["mean", "std"], "memory_mb": ["mean", "std"]})
            .round(4)
        )
        print(resumo)

        # Estatísticas gerais
        print("\n📈 Estatísticas gerais:")
        print(f"Cenários: {sorted(df_consolidado['scenario'].unique())}")
        print(f"Engines: {sorted(df_consolidado['engine'].unique())}")
        print(f"Operações: {sorted(df_consolidado['operation'].unique())}")
    else:
        print("Nenhum dado válido encontrado para consolidação")


# Executar consolidação
print("\n" + "=" * 50)
print("CONSOLIDANDO RESULTADOS")
print("=" * 50)
consolidar_resultados()
