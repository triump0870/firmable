import os
import os
import re
import subprocess
import zipfile
from time import sleep

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine

from scripts.common import get_existing_records_count, remove_duplicates, get_engine


def count_nodes(file_name, node_type):
    node_regex = "</" + node_type + ">|<" + node_type + "/>|<" + node_type + " />"
    egrep = subprocess.Popen(['egrep', '-o', node_regex, file_name], stdout=subprocess.PIPE)
    count = subprocess.Popen(['wc', '-l'],
                             stdin=egrep.stdout,
                             stdout=subprocess.PIPE,
                             )
    end_of_pipe = count.stdout
    result = int(re.sub('[\n\s\r\t]*', '', end_of_pipe.read().decode()))
    return result


def xslt_process_to_pg(table_name, engine, p, schema):
    # The first line has the variable names ...
    var_names = p.stdout.readline().rstrip().lower().split(sep="\t")
    # ... the rest is the data
    copy_cmd = "COPY " + f"{schema}." + table_name + " (" + ", ".join(var_names) + ")"
    copy_cmd += " FROM STDIN CSV DELIMITER E'\\t' QUOTE E'\\b' ENCODING 'utf-8';"

    connection = engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.copy_expert(copy_cmd, p.stdout)
        cursor.close()
        connection.commit()
        result = True
        error = None
    except Exception as e:
        result = False
        error = e
    finally:
        connection.close()
        p.stdout.close()

    return result, error


def get_xslt_process(path, table_name):
    data_dir = os.getenv("DATA_DIR", 'data')
    xsl_file = data_dir + "/xml_to_csv_" + table_name + ".xsl"

    err = open(data_dir + '/err.txt', 'w')
    xslt = subprocess.Popen(["xsltproc", xsl_file, path], stdout=subprocess.PIPE, stderr=err, universal_newlines=True)
    return (xslt)


def write_table_data_from_xml_file(path, table_name, engine, schema):
    xslt = get_xslt_process(path, table_name)
    result, error = xslt_process_to_pg(table_name, engine, xslt, schema)
    return result, error


def process_xml_file(path, engine, schema):
    # First count the number of the fundamental nodes for each table: ABR for abns, OtherEntity for trading_names
    # DGR for dgr. These correspond to the number of rows that should be read into each table from the file in path

    numrows_abns = count_nodes(path, 'ABR')
    numrows_trading_names = count_nodes(path, 'OtherEntity')
    numrows_dgr = count_nodes(path, 'DGR')

    # Note: this assumes path being the full path to the file, ie. directory/file_name

    num_abns_before = get_existing_records_count(schema, "abns", engine)
    num_trading_names_before = get_existing_records_count(schema, "trading_name", engine)
    num_dgr_before = get_existing_records_count(schema, 'dgr', engine)

    success_abns, write_abns_error = write_table_data_from_xml_file(path, 'abns', engine, schema)
    success_trading_names, write_trading_names_error = write_table_data_from_xml_file(path, 'trading_names', engine,
                                                                                      schema)
    success_dgr, write_dgr_error = write_table_data_from_xml_file(path, 'dgr', engine, schema)

    num_abns_after = get_existing_records_count(schema, "abns", engine)
    num_trading_names_after = get_existing_records_count(schema, "trading_name", engine)
    num_dgr_after = get_existing_records_count(schema, 'dgr', engine)

    num_abns_written = num_abns_after - num_abns_before
    num_trading_names_written = num_trading_names_after - num_trading_names_before
    num_dgr_written = num_dgr_after - num_dgr_before

    if not (success_abns and num_abns_written == numrows_abns):
        if num_abns_written > 0:
            print("Error in process_xml_file: expected to write " + str(numrows_abns) + \
                  f" records to {schema}.abns from " + path + ", " + num_abns_written + " were written")
        else:
            print("Error in process_xml_file: expected to write " + str(numrows_abns) + \
                  f" records to {schema}.abns from " + path + ", 0 were written")

    if not (success_trading_names and num_trading_names_written == numrows_trading_names):
        if num_trading_names_written > 0:
            print("Error in process_xml_file: expected to write " + str(numrows_trading_names) + \
                  " records to abn_lookup.trading_names from " + path + ", " + num_trading_names_written + " were written")
        else:
            print("Error in process_xml_file: expected to write " + str(numrows_trading_names) + \
                  " records to abn_lookup.trading_names from " + path + ", 0 were written")

    if not (success_dgr and num_dgr_written == numrows_dgr):
        if num_dgr_written > 0:
            print("Error in process_xml_file: expected to write " + str(numrows_dgr) + \
                  " records to abn_lookup.dgr from " + path + ", " + num_dgr_written + " were written")
        else:
            print("Error in process_xml_file: expected to write " + str(numrows_dgr) + \
                  " records to abn_lookup.dgr from " + path + ", 0 were written")
            return False

    if write_abns_error is None and write_trading_names_error is None and write_dgr_error is None:
        if success_abns and success_trading_names and success_dgr:
            return True
        else:
            return False
    else:

        if write_abns_error:
            print("From write_table_data_from_xml_file('" + path + "', 'abns'): ")
            print(write_abns_error)
        if write_trading_names_error:
            print("From write_table_data_from_xml_file('" + path + "', 'trading_names'): ")
            print(write_trading_names_error)
        if write_trading_names_error:
            print("From write_table_data_from_xml_file('" + path + "', 'dgr'): ")
            print(write_dgr_error)
        return False


def delete_full_directory(directory):
    # Warning: assumes no subfolders. Designed specifically to delete xml_files folder within the abn_lookup folder
    for file in os.listdir(directory):
        os.remove(directory + '/' + file)

    os.rmdir(directory)


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


def make_new_tables(engine, schema):
    # Rename the current tables to be old tables (to be renamed back if data writing into new tables fails later on)

    engine.execute(f'ALTER TABLE IF EXISTS {schema}.abns RENAME TO abns_old')
    engine.execute(f'ALTER TABLE IF EXISTS {schema}.trading_names RENAME TO trading_names_old')
    engine.execute(f'ALTER TABLE IF EXISTS {schema}.dgr RENAME TO dgr_old')

    make_abns_sql = f"""CREATE TABLE {schema}.abns (
                          abn BIGINT,
                          abn_status TEXT,
                          abn_status_from_date DATE,
                          record_last_updated_date DATE,
                          replaced TEXT,
                          entity_type_ind TEXT,
                          entity_type_text TEXT,
                          asic_number TEXT,
                          asic_number_type TEXT,
                          gst_status TEXT,
                          gst_status_from_date DATE,
                          main_ent_type TEXT,
                          main_ent_name TEXT,
                          main_ent_add_state TEXT,
                          main_ent_add_postcode TEXT,
                          legal_ent_type TEXT,
                          legal_ent_title TEXT, 
                          legal_ent_family_name TEXT, 
                          legal_ent_given_names TEXT, 
                          legal_ent_add_state TEXT, 
                          legal_ent_add_postcode TEXT
                        );
                    """

    make_trading_names_sql = f"""CREATE TABLE {schema}.trading_names (

                                  abn BIGINT,
                                  name TEXT,
                                  "type" TEXT
                                );
                             """

    make_dgr_sql = f"""CREATE TABLE {schema}.dgr (
                          abn BIGINT,
                          name TEXT,
                          "type" TEXT,
                          dgr_status_from_date DATE

                        );
                   """

    # Now instantiate new tables

    engine.execute(make_abns_sql)
    engine.execute(make_trading_names_sql)
    engine.execute(make_dgr_sql)

    # Finally, set ownership and access for new tables

    set_table_ownership_access('abns', schema, engine)
    set_table_ownership_access('trading_names', schema, engine)
    set_table_ownership_access('dgr', schema, engine)


def download_xml_files(download_dir):
    if (os.path.exists(download_dir)):
        delete_full_directory(download_dir)

    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
    })

    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    command_result = driver.execute("send_command", params)

    driver.get('http://data.gov.au/dataset/abn-bulk-extract')
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    xml_file_list = []
    for button in soup.findAll(attrs={'class': "download-button au-btn au-btn--secondary"}):
        href = button.get('href')
        if (re.search('.zip$', href)):
            xml_file_list.append(href)

    for file_url in xml_file_list:
        time = 0
        driver.get(file_url)
        file_name = re.search('[0-9a-zA-Z\_]+.zip$', file_url).group(0)
        sleep(2)
        while (os.path.isfile(download_dir + '/' + file_name + ".crdownload")):
            sleep(0.02)
            time = time + 0.02
            if (time >= 600):
                print("Error: file " + file_name + " failed to completely download. Halting. Try again.")
                driver.quit()
                return False

        if (not os.path.isfile(download_dir + '/' + file_name)):
            print("Error: file " + file_name + " did not download at all. Halting. Try again.")
            driver.quit()
            return False

    driver.quit()

    for file_url in xml_file_list:
        file_name = re.search('[0-9a-zA-Z\_]+.zip$', file_url).group(0)
        with zipfile.ZipFile(download_dir + '/' + file_name, 'r') as zip_ref:
            zip_ref.extractall(download_dir)
        os.remove(download_dir + '/' + file_name)

    return True


def main():
    engine = get_engine()
    schema = 'abn_lookup'
    abn_lookup_dir = os.getenv("DATA_DIR", "data")
    download_dir = abn_lookup_dir + "/xml_files"
    xml_file_names = os.listdir(download_dir)

    if not os.path.exists(download_dir) and not xml_file_names:
        download_xml_files(download_dir)
        make_new_tables(engine, schema)

    xml_paths = [os.path.join(download_dir, name) for name in xml_file_names]
    success = False

    for i, path in enumerate(xml_paths[:2]):
        print(f"Processing {xml_file_names[i]}")
        success = process_xml_file(path, engine, schema)
        if success:
            os.remove(path)
            print("File processed successfully")
        else:
            print("Failed to process file completely, halting execution")
            break

    if success:
        # Fix gst_status_from_date for entries with gst_status set to 'NON' (we want it NULL here, not set to 1900-01-01)
        engine.execute(f"UPDATE {schema}.abns SET gst_status_from_date = NULL WHERE gst_status = 'NON'")

        # Remove the duplicate trading names
        remove_duplicates(schema, 'trading_names', ['abn', 'name', 'type'], 'abn', engine)

        # Making of new tables was successful, drop old tables

        engine.execute(f"DROP TABLE IF EXISTS {schema}.abns_old")
        engine.execute(f"DROP TABLE IF EXISTS {schema}.trading_names_old")
        engine.execute(f"DROP TABLE IF EXISTS {schema}.dgr_old")

        os.rmdir(download_dir)
    else:
        # success is only false here if for some xml file, process_xml_file returned false, leading to the loop being broken
        # (and success not being subsequently updated)
        # In this case, remove the incomplete tables, rename the old ones back to the proper names

        engine.execute(f"DROP TABLE IF EXISTS {schema}.abns")
        engine.execute(f"DROP TABLE IF EXISTS {schema}.trading_names")
        engine.execute(f"DROP TABLE IF EXISTS {schema}.dgr")

        engine.execute(f"ALTER TABLE IF EXISTS {schema}.abns_old RENAME TO abns")
        engine.execute(f"ALTER TABLE IF EXISTS {schema}.trading_names_old RENAME TO trading_names")
        engine.execute(f"ALTER TABLE IF EXISTS {schema}.dgr_old RENAME TO dgr")

    engine.dispose()


if __name__ == "__main__":
    main()
