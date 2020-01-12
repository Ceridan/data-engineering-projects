import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Load raw data from source to staging tables in DWH (Amazon Redshift).
        Raw data stored in the Amazon S3 storage in JSON format.

        JSON files are processed and copy into two staging tables:
        - `staging.songs` with metadata about songs and artists.
        - `staging,events` with raw events from Sparkify service with information about user activity.
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Load data from staging tables to dimension and fact tables.
        This step includes data quality checks.
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Connect to Amazon Redshift cluster and process raw data from source in two steps:
        - Load raw data in JSON format from Amazon S3 to staging tables in the DWH (Amazon Redshift).
        - Load data from staging tables to dimension and fact tables in the DWH (include data quality checks).
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
