import pyodbc

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
    return pyodbc.connect(CONNECTION_STRING)
