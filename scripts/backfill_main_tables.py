from common import get_engine

from tables import ABNS


def load_data_to_main_table(staging_table_name, main_table_name, other_columns, engine):
    with engine.connect() as conn:
        # Cast ABN to bigint and insert into main table
        set_clause = ', '.join([f"{field} = EXCLUDED.{field}" for field in other_columns])

        sql_query = f"""
                    INSERT INTO {main_table_name} (abn, record_last_updated_date, {",".join(other_columns)})
                    SELECT
                        s.abn, s.record_last_updated_date, {",".join([f"s.{i}" for i in other_columns])}
                    FROM
                        {staging_table_name} s
                    ON CONFLICT (abn, record_last_updated_date) DO NOTHING
                """
        conn.execute(sql_query)


def main():
    # load abn_lookup.abns data to abn_lookup.abns_main
    engine = get_engine()
    columns = ABNS.__table__.columns
    # Get the fields as a list except abn and record_last_updated_date
    fields = [column.name for column in columns if column.name not in ['abn', 'record_last_updated_date']]
    load_data_to_main_table('abn_lookup.abns', 'abn_lookup.abns_main', fields, engine)


if __name__ == "__main__":
    main()
