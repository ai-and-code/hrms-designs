import pandas as pd
import mysql.connector
from mysql.connector import Error


def get_db_connection(host, user, password, database):
    """
    Establishes and returns a MySQL database connection.
    
    Args:
        host (str): Database host address
        user (str): Database username
        password (str): Database password
        database (str): Database name
    
    Returns:
        connection object or None if connection fails
    """
    try:
        return mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4'
        )
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None


def get_all_tables(connection):
    """
    Retrieves a list of all table names in the database.
    
    Args:
        connection: MySQL database connection object
    
    Returns:
        List of table names or empty list if error occurs
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        return tables
    except Error as e:
        print(f"Error fetching table names: {e}")
        return []


def export_all_tables_to_excel(host, user, password, database, excel_file_path):
    """
    Exports all tables from a MySQL database to a single Excel file.
    Each table is saved as a separate sheet with the table name.
    
    Args:
        host (str): Database host address
        user (str): Database username
        password (str): Database password
        database (str): Database name
        excel_file_path (str): Path to the output Excel file
    """
    connection = get_db_connection(host, user, password, database)
    if connection is None:
        print("Failed to get a database connection. Exiting.")
        return

    try:
        # Get all table names
        tables = get_all_tables(connection)
        
        if not tables:
            print("No tables found in the database.")
            return
        
        print(f"Found {len(tables)} table(s) in database '{database}':")
        for table in tables:
            print(f"  - {table}")
        
        # Create an Excel writer object
        with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
            for table_name in tables:
                try:
                    # Read data from each table
                    sql_query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql(sql_query, connection)
                    
                    # Write to Excel sheet with table name
                    # Excel sheet names have a 31 character limit
                    sheet_name = table_name[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    print(f"Exported table '{table_name}' ({len(df)} rows) to sheet '{sheet_name}'")
                    
                except Exception as e:
                    print(f"Error exporting table '{table_name}': {e}")
                    continue
        
        print(f"\nSuccessfully exported all tables to '{excel_file_path}'")
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
    finally:
        if connection.is_connected():
            connection.close()
            print("Database connection closed.")


def main():
    """
    Main function to get user input and export all database tables to Excel.
    """
    print("=" * 60)
    print("MySQL Database to Excel Exporter")
    print("=" * 60)
    
    # Get user inputs
    host = input("Enter database host (default: localhost): ").strip() or 'localhost'
    database = input("Enter database name: ").strip()
    user = input("Enter database username: ").strip()
    password = input("Enter database password: ").strip()
    
    if not database or not user:
        print("Error: Database name and username are required!")
        return
    
    # Set output file name
    output_file = f"{database}_export.xlsx"
    custom_output = input(f"Enter output file name (default: {output_file}): ").strip()
    if custom_output:
        if not custom_output.endswith('.xlsx'):
            custom_output += '.xlsx'
        output_file = custom_output
    
    print("\nConnecting to database and exporting tables...")
    print("-" * 60)
    
    # Export all tables
    export_all_tables_to_excel(host, user, password, database, output_file)


if __name__ == "__main__":
    main()
