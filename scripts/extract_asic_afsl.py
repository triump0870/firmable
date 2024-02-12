import csv
import os

import pandas as pd

from common import set_table_ownership_access, import_csv_to_database, download_csv_file, get_engine


def make_new_tables(engine, schema):
    # Rename the current tables to be old tables (to be renamed back if data writing into new tables fails later on)
    engine.execute(f'ALTER TABLE IF EXISTS {schema}.afsl RENAME TO afsl_old')
    make_afsl_sql = f"""CREATE TABLE {schema}.afsl (
                        register_name TEXT,
                        afs_lic_num TEXT,
                        afs_lic_name TEXT,
                        afs_lic_abn_acn TEXT,
                        afs_lic_start_dt TEXT,
                        afs_lic_pre_fsr TEXT,
                        afs_lic_add_local TEXT,
                        afs_lic_add_state TEXT,
                        afs_lic_add_pcode TEXT,
                        afs_lic_add_country TEXT,
                        afs_lic_lat TEXT,
                        afs_lic_lng TEXT,
                        afs_lic_condition TEXT
                    );
                    """
    engine.execute(make_afsl_sql)
    # Finally, set ownership and access for new tables
    set_table_ownership_access('afsl', schema, engine)


def count_csv_entries(file_path):
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        # Skip the header row
        next(reader)
        # Count the remaining lines
        line_count = sum(1 for row in reader)
    return line_count


def process_csv_file(path, engine, schema):
    numrows = count_csv_entries(path)

    num_afsl_before = pd.read_sql(f'SELECT COUNT(*) AS count FROM {schema}.afsl', engine).loc[0, 'count']
    success_afsl, write_afsl_error = import_csv_to_database(path, schema, "afsl", engine)
    num_afsl_after = pd.read_sql(f'SELECT COUNT(*) AS count FROM {schema}.afsl', engine).loc[0, 'count']
    num_afsl_written = num_afsl_after - num_afsl_before

    if not (success_afsl and num_afsl_written == numrows):
        if num_afsl_written > 0:
            print("Error in csv file: expected to write " + str(numrows) + \
                  f" records to {schema}.afsl from " + path + ", " + str(num_afsl_written) + " were written")
        else:
            print("Error in csv file: expected to write " + str(numrows) + \
                  f" records to {schema}.afsl from " + path + ", 0 were written")

    if write_afsl_error is None:
        if success_afsl:
            return True
        else:
            return False
    else:
        if write_afsl_error:
            print("From import_csv_to_database('" + path + "', 'afsl'): ")
            print(write_afsl_error)
        return False


def main():
    engine = get_engine()

    data_dir = os.getenv("DATA_DIR", "data")
    download_dir = data_dir + "/csv_files"
    os.makedirs(download_dir, exist_ok=True)

    # Usage example
    url = "https://data.gov.au/data/dataset/ab7eddce-84df-4098-bc8f-500d0d9776d1/resource/d98a113d-6b50-40e6-b65f-2612efc877f4/download/afs_lic_202402.csv"
    output_file = download_dir + "/afsl_export.csv"

    if download_csv_file(url, output_file):
        make_new_tables(engine, schema='afsl_lookup')
        print("The file was downloaded successfully")
        success = process_csv_file(output_file, engine, schema='afsl_lookup')
        if success:
            print("File processed successfully")
        else:
            print("Failed to process file completely, halting execution")
    else:
        # Handle the case when file download fails
        print("File download was failed")


if __name__ == "__main__":
    main()
