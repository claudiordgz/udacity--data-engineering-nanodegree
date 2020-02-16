import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from utilities import get_password_from_source

def load_staging_tables(cur, conn):
    """Will run load staging table queries that move data from s3 to staging tables
    
    Positional arguments:
    cur -- psycopg2 Cursor to Redshift
    conn -- psycopg2 Connection to Redshift
    """
    print('Loading Staging Tables')
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print('Error in: ', query)
            print(e)


def insert_tables(cur, conn):
    """Will run insert queries that copy data from staging tables
    
    Positional arguments:
    cur -- psycopg2 Cursor to Redshift
    conn -- psycopg2 Connection to Redshift
    """
    print('Moving from Staging')
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print('Error in: ', query)
            print(e)

def main():
    """Will load configuration (dwh.cfg).
    
    Note on Password:
    
      - To load password from ssm, use the prefix ssm://, e.g., if your
        password is stored in a SecureString Parameter named "password", use ssm://password.
      - To load password from file, use the prefix file:// using a relative or full path.
      - If password has no prefix it will return as is.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST = config.get('CLUSTER', 'HOST')
    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = get_password_from_source(config.get('CLUSTER', 'DB_PASSWORD'))
    DB_PORT = config.get('CLUSTER', 'DB_PORT')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        HOST,
        DB_NAME,
        DB_USER,
        DB_PASSWORD,
        DB_PORT
    ))
    cur = conn.cursor()
    
    # load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()