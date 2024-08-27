import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import csv

# URL to scrape
url = 'https://screener.in/company/RELIANCE/consolidated/'

# Send get request to url and stores in webpage
webpage = requests.get(url)
soup = bs(webpage.text, 'html.parser')

# Find the profit-loss section and table
data = soup.find('section', id="profit-loss")
tdata = data.find("table")

# Extract table data
table_data = []
for row in tdata.find_all('tr'):
    row_data = []
    for cell in row.find_all(['th', 'td']):
        row_data.append(cell.text.strip())
    table_data.append(row_data)

# Create DataFrame from table data
df_table = pd.DataFrame(table_data)
# Rename the first cell in first row
df_table.iloc[0, 0] = 'Section'
# Sets the first row of the df as column names
df_table.columns = df_table.iloc[0]
# Removes the first row from df
df_table = df_table[1:]

# Ensure only valid numeric data is processed with eval
def safe_eval(val):
    try:
        return eval(val)
    except:
        return val

# It iterates over all columns except the first row
for i in df_table.iloc[:, 1:].columns:
    df_table[i] = df_table[i].str.replace(',', '').str.replace('%', '/100').apply(safe_eval)

print(df_table)
