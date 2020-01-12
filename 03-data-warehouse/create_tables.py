import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries


def create_schemas(cur, conn):
    """
        Execute SQL-statements to create schemas in sparkifydb.

        There are two schemas used in DWH (Amazon Redshift):
        - `staging` schema to land the raw data loaded from the source.
        - `public` (default schema) with dimension and fact tables prepared for analytic queries.
    """

    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    """Execute SQL-statements to drop the existing tables in sparkifydb in case to recreate all tables from scratch."""

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Execute SQL-statements to create all required tables for sparkifydb."""

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connect to Amazon Redshift cluster and recreate sparkifydb database from scratch."""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    create_schemas(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
