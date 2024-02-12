import os
from datetime import datetime
from typing import List

import requests
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, TIMESTAMP
import pandas as pd


def get_engine():
    dbname = os.getenv("PGDATABASE", "firmable")
    host = os.getenv("PGHOST", "localhost")
    conn_string = "postgresql://" + host + "/" + dbname
    engine = create_engine(conn_string)
    return engine


def set_table_ownership_access(table_name, schema, engine):
    owner = schema
    access = schema + "_access"

    line1 = "SET search_path TO " + schema + ";"
    line2 = "ALTER TABLE " + table_name + " OWNER TO " + owner + ";"
    line3 = "GRANT SELECT ON " + table_name + " TO " + access + ";"

    sql = line1 + "\n\t\n" + line2 + "\n\t\n" + line3

    connection = engine.connect()
    trans = connection.begin()
    connection.execute(sql)
    trans.commit()
    connection.close()


def import_csv_to_database(csv_file_path, schema, table_name, engine, delimiter=",", header=True):
    """Import data from a CSV file into a PostgreSQL database table."""
    connection = engine.raw_connection()
    try:
        cur = connection.cursor()
        copy_query = f"COPY {schema}.{table_name} FROM STDIN DELIMITER '{delimiter}' CSV"
        if header:
            copy_query += " HEADER"
        with open(csv_file_path, "rb") as f:
            cur.copy_expert(sql=copy_query, file=f)
        connection.commit()
        print("Data imported successfully.")
        cur.close()
        result = True
        error = None
    except Exception as e:
        print("Error:", e)
        connection.rollback()
        result = False
        error = e
    finally:
        connection.close()
    return result, error


def download_csv_file(url, output_file):
    """
    Download a file from the specified URL and save it locally.

    Args:
        url (str): The URL of the file to download.
        output_file (str): The path to save the downloaded file.

    Returns:
        bool: True if the file was downloaded successfully, False otherwise.
    """
    try:
        # Send a GET request to the URL
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Write the content of the response to a local file
            with open(output_file, 'wb') as f:
                f.write(response.content)
            print("File downloaded successfully.")
            return True
        else:
            print("Failed to download the file. Status code:", response.status_code)
            return False
    except Exception as e:
        print("An error occurred during file download:", e)
        return False


def get_existing_records_count(schema, table, engine):
    count = pd.read_sql(f'SELECT COUNT(*) AS count FROM {schema}.{table}', engine).loc[0, 'count']
    return count


def remove_duplicates(schema, table, fields: List, order_field, engine):
    sql_command = f"""
                    WITH duplicates AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {",".join(fields)} ORDER BY {order_field}) AS row_num
                    FROM {schema}.{table}
                    )
                    DELETE FROM {schema}.{table}
                    WHERE ({",".join(fields)}) IN (
                        SELECT {",".join(fields)}
                        FROM duplicates
                        WHERE row_num > 1
                    );
                """
    with engine.connect() as connection:
        connection.execute(sql_command)


def update_extraction_info(engine, data_source, total_records, new_records, updated_records=0):
    sql = """

    """
    metadata = MetaData()
    extraction_info = Table(
        'extraction_info',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('data_source_name', String),
        Column('extraction_dt', TIMESTAMP),
        Column('total_records', Integer),
        Column('new_records', Integer),
        Column('updated_records', Integer)
    )
    timestamp = datetime.now()
    with engine.connect() as connection:
        # Insert data into the table
        insert_statement = extraction_info.insert().values(
            data_source_name=data_source,
            extraction_dt=timestamp,
            total_records=total_records,
            new_records=new_records,
            updated_records=updated_records
        )
        connection.execute(insert_statement)
