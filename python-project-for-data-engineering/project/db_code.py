import sqlite3
import pandas as pd
conn = sqlite3.connect("STAFF.db")

table_name = "INSTRUCTOR"
attribute_list = ["ID", "FNAME", "LNAME", "CITY", "CCODE"]

file_path = "./INSTRUCTOR.csv"
df = pd.read_csv(file_path, names=attribute_list)

df.to_sql(table_name, conn, if_exists="replace", index=False)
print("Table is already!")

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

data_dict = {"ID": [100], "FNAME": ["John"], "LNAME": ["Joe"], "CITY": ["Paris"], "CCODE": ["FR"]}
data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_name, conn, if_exists="append", index=False)
print("Append data successfully!")

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()