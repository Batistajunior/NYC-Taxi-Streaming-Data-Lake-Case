import os

import pandas as pd

from src.utils.config import SILVER_PATH, GOLD_PATH
from src.utils.logger import get_logger


logger = get_logger(__name__)


def print_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def analyze_dataframe(df: pd.DataFrame, dataset_name: str) -> None:
    print_section(f"ANÁLISE DO DATASET: {dataset_name}")

    print("\n1. DIMENSÃO DO DATASET")
    print(f"Total de linhas: {df.shape[0]:,}")
    print(f"Total de colunas: {df.shape[1]:,}")

    print("\n2. NOMES DAS COLUNAS")
    for col in df.columns:
        print(f"- {col}")

    print("\n3. TIPOS DE DADOS")
    print(df.dtypes)

    print("\n4. QUANTIDADE DE COLUNAS POR TIPO")
    print(df.dtypes.value_counts())

    print("\n5. COLUNAS NUMÉRICAS")
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    print(numeric_cols if numeric_cols else "Nenhuma coluna numérica encontrada.")

    print("\n6. COLUNAS DE TEXTO / STRING")
    string_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
    print(string_cols if string_cols else "Nenhuma coluna string encontrada.")

    print("\n7. COLUNAS DE DATA / DATETIME")
    datetime_cols = df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
    print(datetime_cols if datetime_cols else "Nenhuma coluna datetime encontrada.")

    print("\n8. VALORES NULOS POR COLUNA")
    nulls = df.isna().sum()
    nulls_percent = (df.isna().mean() * 100).round(2)

    null_report = pd.DataFrame(
        {
            "coluna": df.columns,
            "tipo": df.dtypes.astype(str).values,
            "qtd_nulos": nulls.values,
            "perc_nulos": nulls_percent.values,
            "qtd_nao_nulos": df.notna().sum().values,
        }
    )

    print(null_report.sort_values(by="qtd_nulos", ascending=False).to_string(index=False))

    print("\n9. COLUNAS COM VALORES NULOS")
    cols_with_nulls = null_report[null_report["qtd_nulos"] > 0]

    if cols_with_nulls.empty:
        print("Nenhuma coluna possui valores nulos.")
    else:
        print(cols_with_nulls.to_string(index=False))

    print("\n10. VALORES VAZIOS EM COLUNAS STRING")
    if string_cols:
        empty_string_report = []

        for col in string_cols:
            empty_count = df[col].astype(str).str.strip().eq("").sum()
            empty_percent = round((empty_count / len(df)) * 100, 2)

            empty_string_report.append(
                {
                    "coluna": col,
                    "qtd_vazios": empty_count,
                    "perc_vazios": empty_percent,
                }
            )

        print(pd.DataFrame(empty_string_report).to_string(index=False))
    else:
        print("Nenhuma coluna string para verificar valores vazios.")

    print("\n11. QUANTIDADE DE VALORES ÚNICOS POR COLUNA")
    unique_report = pd.DataFrame(
        {
            "coluna": df.columns,
            "tipo": df.dtypes.astype(str).values,
            "qtd_valores_unicos": df.nunique(dropna=True).values,
        }
    )

    print(unique_report.sort_values(by="qtd_valores_unicos", ascending=False).to_string(index=False))

    print("\n12. REGISTROS DUPLICADOS")
    duplicated_count = df.duplicated().sum()
    duplicated_percent = round((duplicated_count / len(df)) * 100, 2)

    print(f"Quantidade de linhas duplicadas: {duplicated_count:,}")
    print(f"Percentual de duplicadas: {duplicated_percent}%")

    print("\n13. ESTATÍSTICAS DAS COLUNAS NUMÉRICAS")
    if numeric_cols:
        print(df[numeric_cols].describe().T.to_string())
    else:
        print("Nenhuma coluna numérica encontrada.")

    print("\n14. ESTATÍSTICAS DAS COLUNAS STRING")
    if string_cols:
        print(df[string_cols].describe().T.to_string())
    else:
        print("Nenhuma coluna string encontrada.")

    print("\n15. PRIMEIRAS 10 LINHAS")
    print(df.head(10).to_string(index=False))

    print("\n16. ÚLTIMAS 10 LINHAS")
    print(df.tail(10).to_string(index=False))

    print("\n17. CONSUMO DE MEMÓRIA")
    memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"Memória utilizada pelo DataFrame: {memory_mb:.2f} MB")


def main() -> None:
    logger.info("Starting exploratory data analysis")

    silver_file = os.path.join(SILVER_PATH, "silver_taxi_trips.parquet")
    gold_file = os.path.join(GOLD_PATH, "gold_metrics.parquet")

    if os.path.exists(silver_file):
        logger.info(f"Reading silver file: {silver_file}")
        df_silver = pd.read_parquet(silver_file)
        analyze_dataframe(df_silver, "SILVER - silver_taxi_trips.parquet")
    else:
        logger.warning(f"Silver file not found: {silver_file}")

    if os.path.exists(gold_file):
        logger.info(f"Reading gold file: {gold_file}")
        df_gold = pd.read_parquet(gold_file)
        analyze_dataframe(df_gold, "GOLD - gold_metrics.parquet")
    else:
        logger.warning(f"Gold file not found: {gold_file}")


if __name__ == "__main__":
    main()
