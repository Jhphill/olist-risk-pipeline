import psycopg2
import sys

try:
    conn = psycopg2.connect("postgresql://olist_user:olist_pass@localhost:5432/olist_db")
    print("Conexion exitosa")
    conn.close()
except Exception as e:
    print("Error:", repr(e))
    print("Tipo:", type(e))