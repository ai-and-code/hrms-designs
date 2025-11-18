import pandas as pd
import mysql.connector
import argparse
import getpass
import os

def get_db_connection(host, user, password, database):
    """
    Establishes and returns a MySQL database connection using provided parameters.
    """
    try:
        return mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4'
        )
    except mysql.connector.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def export_table_to_excel(connection, table_name):
    """
    Fetches all data from a specified MySQL table and exports it to an Excel file
    with the same name as the table.
    """
    excel_file_path = f"{table_name}.xlsx"
    
    try:
        sql_query = f"SELECT * FROM `{table_name}`"
        df = pd.read_sql(sql_query, connection)
        df.to_excel(excel_file_path, index=False, engine='openpyxl')
        print(f"Successfully exported data from '{table_name}' to '{excel_file_path}'")
    except pd.io.sql.DatabaseError as e:
        print(f"Error executing query or reading data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export a MySQL table to an Excel file.")
    parser.add_argument("--host", required=True, help="Database host (e.g., localhost).")
    parser.add_argument("--user", required=True, help="Database username.")
    parser.add_argument("--database", required=True, help="Database name.")
    parser.add_argument("--table", required=True, help="Name of the table to export.")
    
    args = parser.parse_args()

    # Securely get the password input without displaying it on the terminal
    db_password = getpass.getpass(prompt="Enter database password: ")

    connection = get_db_connection(
        host=args.host,
        user=args.user,
        password=db_password,
        database=args.database
    )

    if connection:
        export_table_to_excel(
            connection,
            table_name=args.table
        )
    # Example parameters; in practice, use command-line arguments or environment variables
    # export_table_to_excel('your_table_name', 'output.xlsx')