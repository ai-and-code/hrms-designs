import pandas as pd
import mysql.connector
from mysql.connector import errorcode
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
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return None

def import_excel_to_table(connection, table_name, excel_file_path):
    """
    Reads data from an Excel file and inserts it into a specified MySQL table.
    """
    try:
        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(excel_file_path)
        
        # Get a cursor object
        cursor = connection.cursor()
        
        # Prepare the INSERT statement
        # The number of %s placeholders must match the number of columns in the DataFrame
        columns = ', '.join(f'`{col}`' for col in df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        
        # Iterate over DataFrame rows and insert into the database
        for index, row in df.iterrows():
            # Convert row to a tuple for the cursor.execute method
            values = tuple(row)
            cursor.execute(sql, values)
        
        # Commit the changes to the database
        connection.commit()
        
        print(f"Successfully imported data from '{excel_file_path}' into table '{table_name}'.")
        print(f"{cursor.rowcount} rows were inserted.")
        
    except FileNotFoundError:
        print(f"Error: The file '{excel_file_path}' was not found.")
    except pd.errors.ParserError as e:
        print(f"Error parsing the Excel file: {e}")
    except mysql.connector.Error as e:
        print(f"Database error during insertion: {e}")
        # Rollback changes on error
        connection.rollback()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import an Excel file into a MySQL table.")
    parser.add_argument("--host", required=True, help="Database host (e.g., localhost).")
    parser.add_argument("--user", required=True, help="Database username.")
    parser.add_argument("--database", required=True, help="Database name.")
    parser.add_argument("--table", required=True, help="Name of the target table.")
    parser.add_argument("--file", required=True, help="Path to the Excel file to import (e.g., data.xlsx).")
    
    args = parser.parse_args()

    # Securely get the password input
    db_password = getpass.getpass(prompt="Enter database password: ")

    connection = get_db_connection(
        host=args.host,
        user=args.user,
        password=db_password,
        database=args.database
    )

    if connection:
        import_excel_to_table(
            connection,
            table_name=args.table,
            excel_file_path=args.file
        )

