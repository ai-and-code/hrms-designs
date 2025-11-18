import pandas as pd
import mysql.connector

# The provided function to get a database connection
def get_db_connection():
    """
    Establishes and returns a MySQL database connection.
    """
    try:
        return mysql.connector.connect(
            host='localhost',
            user='root',
            password='root',
            database='ats',
            charset='utf8mb4'
        )
    except mysql.connector.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def export_table_to_excel(table_name, excel_file_path):
    """
    Fetches all data from a specified MySQL table and exports it to an Excel file.

    Args:
        table_name (str): The name of the MySQL table to export.
        excel_file_path (str): The path to the output Excel file (e.g., 'data.xlsx').
    """
    connection = get_db_connection()
    if connection is None:
        print("Failed to get a database connection. Exiting.")
        return

    try:
        # Use a SQL query to select all data from the table
        sql_query = f"SELECT * FROM {table_name}"
        
        # Read the data into a pandas DataFrame
        # The 'read_sql' function handles both the query execution and fetching data
        df = pd.read_sql(sql_query, connection)
        
        # Write the DataFrame to an Excel file
        df.to_excel(excel_file_path, index=False, engine='openpyxl')
        
        print(f"Successfully exported data from '{table_name}' to '{excel_file_path}'")
        
    except pd.io.sql.DatabaseError as e:
        print(f"Error executing query or reading data: {e}")
    except FileNotFoundError:
        print(f"Error: The specified path '{excel_file_path}' does not exist.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("Database connection closed.")

# --- Example Usage ---
if __name__ == "__main__":
    # You can change 'your_table_name' to the actual table name you want to export
    table_to_export = 'users'
    # You can change 'output_data.xlsx' to your desired output file name
    output_excel_file = 'output_data.xlsx'
    
    export_table_to_excel(table_to_export, output_excel_file)