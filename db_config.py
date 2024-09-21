#!/usr/bin/python3
# db_config.py
import pymysql

def get_db_connection():
    return pymysql.connect(
        host="192.168.178.23",       # e.g., "localhost"
        user="gh",                   # e.g., "root"
        password="a12345",           # e.g., "password"
        database="wagodb"            # e.g., "your_database"
    )
