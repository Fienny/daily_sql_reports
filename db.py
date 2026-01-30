import pyodbc
import psycopg2

# ===========================
# SQL SERVER (Microsoft Fabric)
# ===========================
SERVER = 'i6sgtq7lgkvehkkdrgumfndhvq-66deosrva6ce5jokg5f2ao7gdu.datawarehouse.fabric.microsoft.com'
DATABASE = 'main_lakehouse'
DRIVER = '{ODBC Driver 18 for SQL Server}'

CONNECTION_STRING = f"""
DRIVER={DRIVER};
SERVER={SERVER};
DATABASE={DATABASE};
Authentication=ActiveDirectoryInteractive;
Encrypt=yes;
TrustServerCertificate=no;
Connection Timeout=30;
"""

def get_connection():
    """Get SQL Server connection (Microsoft Fabric)"""
    return pyodbc.connect(CONNECTION_STRING)


# ===========================
# POSTGRESQL (DBD Reports)
# ===========================
PG_CONFIG = {
    'host': '192.168.108.111',
    'port': 5432,
    'database': 'ducp',
    'user': 'asattorov',
    'password': '2#SfG^Tj',
    'connect_timeout': 10,
    'options': '-c statement_timeout=300000'
}

def get_pg_connection():
    """Get PostgreSQL connection for DBD reports"""
    conn = psycopg2.connect(**PG_CONFIG)
    print("PostgreSQL connection established")
    return conn


def close_pg_connection(conn):
    """Close PostgreSQL connection"""
    if conn:
        conn.close()
        print("PostgreSQL connection closed")
