#!/usr/bin/python3
# pivot2db.py

import csv
import pymysql
from db_config import get_db_connection
from datetime import datetime

def truncate_column_name(column_name):
    return column_name.split()[0]

def get_csv_columns(csv_file_path):
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        columns = next(csv_reader)
    return [truncate_column_name(col) for col in columns]

def check_and_update_table(cursor, table_name, columns):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    table_exists = cursor.fetchone()

    if table_exists:
        cursor.execute(f"DESCRIBE {table_name}")
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        expected_columns = set(['id', 'timestamp'] + columns)
        existing_columns_set = set(existing_columns)
        
        if existing_columns_set != expected_columns:
            print(f"Table structure mismatch for {table_name}:")
            missing_columns = expected_columns - existing_columns_set
            extra_columns = existing_columns_set - expected_columns
            
            if missing_columns:
                print(f"Missing columns: {', '.join(missing_columns)}")
                for col in missing_columns:
                    if col not in ['id', 'timestamp']:
                        add_column_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` FLOAT"
                        cursor.execute(add_column_sql)
                        print(f"Added new column: {col}")
            
            if extra_columns:
                print(f"Extra columns: {', '.join(extra_columns)}")
                print("Extra columns will be kept.")
            
            print("Table structure updated.")
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
        print(f"CSV columns (truncated): {columns}")
        
        connection = get_db_connection()
        with connection.cursor() as cursor:
            check_and_update_table(cursor, table_name, columns)
            
            with open(csv_file_path, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    # Truncate column names in the row
                    truncated_row = {truncate_column_name(k): v for k, v in row.items()}
                    truncated_row['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    insert_data(cursor, table_name, truncated_row)
            
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
