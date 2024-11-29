import pandas as pd


def read_transaction_data(filepath: str, row_limit: int, columns: dict):
    df = pd.read_csv(
        filepath,
        nrows=row_limit,
        usecols=columns.keys(),
        dtype=columns,
    )

    df.amount = df.amount.str.replace("$", "", regex=False).astype(float)

    return df
