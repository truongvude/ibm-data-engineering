import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = "World_Economies.db"
table_name = "Countries_by_GDP"
csv_path = "Countries_by_GDP.csv"

page = requests.get(url).text
data = BeautifulSoup(page, "html.parser")
df = pd.DataFrame(columns=table_attribs)
table = data.find_all("tbody")
rows = table[2].find_all("tr")
for row in rows:
    col = row.find_all("td")
    if len(col)!=0:
        if col[0].find("a") is not None and "—" not in col[2]:
            dict = {"Country": col[0].a.contents[0], "GDP_USD_millions": col[2].contents[0]}
            df1 = pd.DataFrame(dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

print(df)