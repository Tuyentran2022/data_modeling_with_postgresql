#TODO 3: Create a database and connect to Postgresql
import psycopg2
import traceback
from sql_queries import Queries


def create_database():
    # Connect to your database already created in psql (SQL Shell)
    try:
        conn = psycopg2.connect(
            user="admin",
            password="<postgresql>", # your password installed for user "admin"
            host="localhost",
            port="5432",
            dbname="data_modeling_postgres_01")

        # Set parameters for next transaction
        conn.set_session(autocommit=True)
        # Open a cursor to perform database operations
        cur = conn.cursor()

    except psycopg2.Error as error:
        message = traceback.format_exc()
        print(f"Error: \n{error}\n")
        print(f"Complete log error: {message}")

    # Create sparkify database with UTF8 encoding (execute "create database")
    cur.execute("DROP DATABASE IF EXISTS sparkifydb;")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0;")

    # close connection
    conn.close()

    # connect to 'sparkifydb' database
    try:
        conn = psycopg2.connect(
            user="admin",
            password="<postgresql>",
            host="localhost",
            port="5432",
            dbname="sparkifydb")
        cur = conn.cursor()

    except psycopg2.Error as error:
        message = traceback.format_exc()
        print(f"Error: \n{error}\n")
        print(f"Complete log error: {message}")

    return cur, conn

def drop_tables(cur,conn):
    sql = Queries()
    for query in sql.drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur,conn):
    sql = Queries()
    for query in sql.create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """pipeline execution
    - Drop if exits and create a 'sparkifydb' database
    - open 'cursor' to perform it
    - drop all tables if they exist and create tables we need
    - close connection
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()

