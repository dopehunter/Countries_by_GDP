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
def data_preparation(html_page):
    soup = BeautifulSoup(html_page, 'html.parser') # BeautifulSoup is a library that allows us to parse HTML and XML documents
    tables = soup.find_all('table')
    print(f"Number of tables: {len(tables)}")
    rows = tables[2].find_all('tr')
    print(f"Number of rows: {len(rows)}")
    return rows

def data_extraction(rows):
    df = pd.DataFrame()
    for row in rows[3:]:
        columns = row.find_all('td')
        if len(columns) != 0:
            try:
                gdp_text = columns[2].get_text().strip()
                gdp_value = round(int(gdp_text.replace('$', '').replace(',', '')) / 1000, 2)
                data_dict = {'Country': [columns[0].get_text().strip()],
                            'GDP_USD_billions': [gdp_value]}
                df1 = pd.DataFrame(data_dict)
                df = pd.concat([df, df1], ignore_index=True)
            except ValueError:
                data_dict = {'Country': [columns[0].get_text().strip()],
                            'GDP_USD_billions': [float('inf')]} # Используем float('inf') для перемещения невалидных значений в конец при сортировке
                df1 = pd.DataFrame(data_dict)
                df = pd.concat([df, df1], ignore_index=True)
        else:
            continue
    return df   

def data_loading(df):
    df.to_csv(csv_file, index=False)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.commit()
def main():
    rows = data_preparation(html_page)
    df = data_extraction(rows)
    data_loading(df)



main()
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()