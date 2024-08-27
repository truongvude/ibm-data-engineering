# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import numpy as np
import sqlite3


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    
    with open(log_file, "a") as f:
        f.write(f"{timestamp}, {message} \n")
    

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, "html.parser")

    table = data.find_all("tbody")
    rows = table[0].find_all("tr")

    df = pd.DataFrame(columns=table_attribs)
    
    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {"Name": col[1].find_all("a")[1].contents[0], "MC_USD_Billion": float(col[2].contents[0])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path)
    dict = exchange_rate.set_index('Currency').to_dict()['Rate']

    df["MC_GBP_Billion"] = [np.round(x*dict["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x*dict["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x*dict["INR"], 2) for x in df["MC_USD_Billion"]]
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    print(pd.read_sql(query_statement, sql_connection))

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Declaring known values
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attributes = ["Name", "MC_USD_Billion"]
output_path = "./Largest_banks_data.csv"
log_file = "code_log.txt"
csv_path = "exchange_rate.csv"
db_name = "Banks.db"
table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")

# Call extract() function
df = extract(url, table_attributes)
log_progress("Data extraction complete. Initiating Transformation process")

# Call transform() function
transform(df, csv_path)
log_progress("Data transformation complete. Initiating Loading process")

# Call load_to_csv()
load_to_csv(df, output_path)
log_progress("Data saved to CSV file")

# Initiate SQLite3 connection
sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

# Call load_to_db()		
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Call run_query
query_statement = "SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = "SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
log_progress("Data loaded to Database as a table, Executing queries")

# Close SQLite3 connection
sql_connection.close()
log_progress("Server Connection closed")