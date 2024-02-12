import csv
import os
import subprocess

import pandas as pd
import requests
from sqlalchemy import create_engine

from common import set_table_ownership_access, import_csv_to_database, download_csv_file, get_engine


def make_new_tables(engine, schema):
    # Rename the current tables to be old tables (to be renamed back if data writing into new tables fails later on)

    engine.execute(f'ALTER TABLE IF EXISTS {schema}.fjcs RENAME TO fjcs_old')

    make_fjcs_sql = f"""CREATE TABLE {schema}.fjcs (
                        certificate_number TEXT,
                        entity_name TEXT,
                        trade_name TEXT, 
                        abn TEXT,
                        status TEXT,
                        issue_date TEXT, 
                        expiry_date TEXT   
                        );
                    """

    engine.execute(make_fjcs_sql)
    # Finally, set ownership and access for new tables
    set_table_ownership_access('fjcs', schema, engine)


def count_csv_entries(file_path):
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        # Skip the header row
        next(reader)
        # Count the remaining lines
        line_count = sum(1 for row in reader)
    return line_count


def process_csv_file(path, engine, schema):
    # First count the number of the fundamental nodes for each table: ABR for abns, OtherEntity for trading_names
    # DGR for dgr. These correspond to the number of rows that should be read into each table from the file in path
    numrows = count_csv_entries(path)

    # Note: this assumes path being the full path to the file, ie. directory/file_name
    num_fjcs_before = pd.read_sql(f'SELECT COUNT(*) AS count FROM {schema}.fjcs', engine).loc[0, 'count']
    success_fjcs, write_fjcs_error = import_csv_to_database(path, schema, "fjcs", engine)
    num_fjcs_after = pd.read_sql(f'SELECT COUNT(*) AS count FROM {schema}.fjcs', engine).loc[0, 'count']
    num_fjcs_written = num_fjcs_after - num_fjcs_before

    if not (success_fjcs and num_fjcs_written == numrows):
        if num_fjcs_written > 0:
            print("Error in csv file: expected to write " + str(numrows) + \
                  f" records to {schema}.fjcs from " + path + ", " + str(num_fjcs_written) + " were written")
        else:
            print("Error in csv file: expected to write " + str(numrows) + \
                  f" records to {schema}.fjcs from " + path + ", 0 were written")

    if write_fjcs_error is None:
        if success_fjcs:
            return True
        else:
            return False
    else:
        if write_fjcs_error:
            print("From import_csv_to_database('" + path + "', 'fjcs'): ")
            print(write_fjcs_error)
        return False


def main():
    engine = get_engine()

    data_dir = os.getenv("DATA_DIR", "data")
    download_dir = data_dir + "/csv_files"
    os.makedirs(download_dir, exist_ok=True)

    # Usage example
    url = "https://fjc-vic-gov.my.salesforce-sites.com/downloadCertExport"
    output_file = download_dir + "/fjc_export.csv"

    if download_csv_file(url, output_file):
        make_new_tables(engine, schema='fjcs_lookup')
        print("The file was downloaded successfully")
        success = process_csv_file(output_file, engine, schema='fjcs_lookup')
        if success:
            print("File processed successfully")
        else:
            print("Failed to process file completely, halting execution")
    else:
        # Handle the case when file download fails
        print("File download was failed")


if __name__ == "__main__":
    main()
