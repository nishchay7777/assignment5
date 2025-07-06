import pandas as pd
import mysql.connector
import pyarrow as pa
import fastavro
from fastavro import writer, parse_schema
import schedule
import time
from sqlalchemy import create_engine

def connect_db():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="nishchay@54321",
            database="sample_db"
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def export_to_formats(table_name):
    engine = create_engine("mysql+mysqlconnector://root:nishchay%4054321@localhost:3306/sample_db")
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)

    csv_file = f"{table_name}.csv"
    df.to_csv(csv_file, index=False)
    print(f"Data exported to {csv_file}")

    parquet_file = f"{table_name}.parquet"
    df.to_parquet(parquet_file, index=False)
    print(f"Data exported to {parquet_file}")

    avro_file = f"{table_name}.avro"
    schema = {
        "type": "record",
        "name": f"{table_name}",
        "fields": [
            {"name": col, "type": ["string", "null"]} for col in df.columns
        ]
    }
    parsed_schema = parse_schema(schema)
    records = df.to_dict('records')
    with open(avro_file, 'wb') as out:
        writer(out, parsed_schema, records)
    print(f"Data exported to {avro_file}")

def run_pipeline():
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        for table in tables:
            export_to_formats(table)
        conn.close()

schedule.every(1).hours.do(run_pipeline)

def check_database_changes():
    conn = connect_db()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT UPDATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA = 'sample_db'")
        if cursor.fetchone():
            run_pipeline()
        conn.close()

def replicate_full_database(source_db, dest_db):
    src_conn = connect_db()
    if src_conn:
        dest_conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="nishchay@54321",
            database=dest_db
        )
        cursor = src_conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        for table in tables:
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, src_conn)
            df.to_sql(table, dest_conn, if_exists='replace', index=False)
        src_conn.close()
        dest_conn.close()
        print(f"Full replication from {source_db} to {dest_db} completed.")

def replicate_selective_tables(source_db, dest_db, tables_cols):
    src_conn = connect_db()
    if src_conn:
        dest_conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="nishchay@54321",
            database=dest_db
        )
        for table, cols in tables_cols.items():
            query = f"SELECT {', '.join(cols)} FROM {table}"
            df = pd.read_sql(query, src_conn)
            df.to_sql(table, dest_conn, if_exists='replace', index=False)
        src_conn.close()
        dest_conn.close()
        print(f"Selective replication from {source_db} to {dest_db} completed.")

if __name__ == "__main__":
    run_pipeline()
    while True:
        schedule.run_pending()
        time.sleep(60)