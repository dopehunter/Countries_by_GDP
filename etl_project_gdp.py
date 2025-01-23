# ETL project for GDP data

# 1. Extract the data from the source
# 2. Transform the data
# 3. Load the data into the target


import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup

# database and table creation
conn = sqlite3.connect('World_Economies.db')
table_name = 'Countries_by_GDP'
table_attributes = ['Country', 'GDP_USD_millions']
# url to the source and response
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
html_page = requests.get(url).text # returns the HTML content of the page

#  output files
csv_file = 'Countries_by_GDP.csv'
log_file = 'etl_project_log.txt'

# parsing the HTML
soup = BeautifulSoup(html_page, 'html.parser') # BeautifulSoup is a library that allows us to parse HTML and XML documents
tables = soup.find_all('table')
rows = tables[2].find_all('tr')
for row in rows:
    print(row.text)


#print(tables[2].text)