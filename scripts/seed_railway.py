import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


TABLES = {
    "tourist_bookings": DATA_DIR / "tourist_bookings.csv",
    "tourists_dimension": DATA_DIR / "tourists_dimension.csv",
    "guides_dimension": DATA_DIR / "guides_dimension.csv",
    "destinations_dimension": DATA_DIR / "destinations_dimension.csv",
    "date_dimension": DATA_DIR / "date_dimension.csv",
    "streaming_events": DATA_DIR / "streaming_events.csv",
    "fact_bookings_stream": DATA_DIR / "tourist_bookings.csv",
}


def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    return psycopg2.connect(database_url, sslmode="require")


def normalize_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def infer_sql_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    return "TEXT"


def load_dataframe(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    for column in df.columns:
        if "date" in column.lower() or "timestamp" in column.lower():
            try:
                df[column] = pd.to_datetime(df[column])
            except (ValueError, TypeError):
                pass
    df = df.replace("NULL", pd.NA)
    return df


def create_table(cursor, table_name: str, df: pd.DataFrame) -> None:
    columns = [
        sql.SQL("{} {}").format(sql.Identifier(column), sql.SQL(infer_sql_type(df[column])))
        for column in df.columns
    ]
    statement = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(columns),
    )
    cursor.execute(statement)


def seed_table(cursor, table_name: str, df: pd.DataFrame) -> None:
    create_table(cursor, table_name, df)
    cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.Identifier(table_name)))

    columns = list(df.columns)
    insert_statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )

    rows = [tuple(normalize_value(value) for value in row) for row in df.itertuples(index=False, name=None)]
    if rows:
        cursor.executemany(insert_statement, rows)


def main():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                for table_name, csv_path in TABLES.items():
                    df = load_dataframe(csv_path)
                    seed_table(cursor, table_name, df)
                    print(f"Seeded {table_name}: {len(df)} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()