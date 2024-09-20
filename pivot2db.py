#!/usr/bin/python3
# pivot2db.py

import csv
import pymysql
from db_config import get_db_connection
from datetime import datetime

def get_csv_columns(csv_file_path):
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        return next(csv_reader)

def check_and_update_table(cursor, table_name, columns):
    # Check if table exists
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    table_exists = cursor.fetchone()

    if table_exists:
        # Check table structure
        cursor.execute(f"DESCRIBE {table_name}")
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        # Compare existing columns with CSV columns
        if set(existing_columns) != set(['id', 'timestamp'] + columns):
            print(f"Table structure mismatch. Dropping and recreating table {table_name}")
            cursor.execute(f"DROP TABLE {table_name}")
            create_table_if_not_exists(cursor, table_name, columns)
        else:
            print(f"Table {table_name} structure is up to date")
    else:
        print(f"Table {table_name} does not exist. Creating it.")
        create_table_if_not_exists(cursor, table_name, columns)

def create_table_if_not_exists(cursor, table_name, columns):
    column_definitions = [
        f"`{col}` FLOAT" if col != "section" else f"`{col}` VARCHAR(255)"
        for col in columns
    ]
    column_definitions = ["id INT AUTO_INCREMENT PRIMARY KEY", "timestamp DATETIME"] + column_definitions
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(column_definitions)}
    )
    """
    cursor.execute(create_table_sql)

def insert_data(cursor, table_name, data):
    columns = ', '.join([f"`{key}`" for key in data.keys()])
    placeholders = ', '.join(['%s'] * len(data))
    sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
    cursor.execute(sql, list(data.values()))

def main():
    csv_file_path = '/tmp/pivoted_registers.csv'
    table_name = 'inverter_data'
    
    try:
        columns = get_csv_columns(csv_file_path)
        print(f"CSV columns: {columns}")
        
        connection = get_db_connection()
        with connection.cursor() as cursor:
            check_and_update_table(cursor, table_name, columns)
            
            with open(csv_file_path, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    row['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    insert_data(cursor, table_name, row)
            
            connection.commit()
            print("Data inserted successfully")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file_path}")
    except pymysql.err.OperationalError as e:
        print(f"Database connection error: {e}")
    except pymysql.err.ProgrammingError as e:
        print(f"SQL error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    main()
