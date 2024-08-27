import requests
from requests.exceptions import ConnectionError
from bs4 import BeautifulSoup as bs
import pandas as pd
import csv
import mysql.connector
from sqlalchemy import create_engine

# URL to scrape
url = 'https://www.screener.in/company/RELIANCE/consolidated/'

try:
    webpage = requests.get(url)
    webpage.raise_for_status()  # Raise an exception for HTTP errors
except ConnectionError as e:
    print(f"Error: {e}")
    exit(1)  # Exit the script with a non-zero status code

# Parse the HTML content
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

# Save table data to csv
with open("table_data.csv", 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(table_data)

# Create DataFrame from table data
df_table = pd.DataFrame(table_data)
# Rename the first cell in first row
df_table.iloc[0, 0] = 'Section'
# Sets the first row of the df as column names
df_table.columns = df_table.iloc[0]
# Removes the first row from df
df_table = df_table[1:]

# Rename the columns
df_table = df_table.rename(columns={'Section': 'Section-Narration'})

# Remove signs from the 'Section-Narration' column
df_table['Section-Narration'] = df_table['Section-Narration'].str.replace('+', '').str.replace('%', '')

# Ensure only valid numeric data is processed with eval
def safe_eval(val):
    try:
        return eval(val)
    except:
        return val

# It iterates over all columns except the first row
for i in df_table.iloc[:, 1:].columns:
    df_table[i] = df_table[i].str.replace(',', '').str.replace('%', '/100').apply(safe_eval)

# MySQL database credentials
db_host = "192.168.3.38"
db_name = "db"
db_user = "root"
db_password = "root"
db_port = "3333"

# Create engine to connect to MySQL database
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load the DataFrame into the MySQL database
df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)

# Pivot the data
pivoted_df = pd.melt(df_table, id_vars=['Section-Narration'], value_vars=df_table.columns[1:], var_name='Category', value_name='Value')

# Separate TTM data from pivoted table
ttm_pivoted_df = pivoted_df[pivoted_df['Category'].str.contains('TTM', case=False, na=False)]

# Remove TTM data from pivoted table
pivoted_df = pivoted_df[~pivoted_df['Category'].str.contains('TTM', case=False, na=False)]

# Rename the 'Value' column in ttm_pivoted_df
ttm_pivoted_df = ttm_pivoted_df.rename(columns={'Value': 'TTM_Value'})

# Pivot the TTM data again
ttm_pivoted_again = pd.melt(ttm_pivoted_df, id_vars=['Section-Narration'], value_vars='TTM_Value', var_name='Category', value_name='Value')

# Load the pivoted DataFrame into the MySQL database
pivoted_df.to_sql('profit_loss_pivoted_data', engine, if_exists='replace', index=False)

# Load the TTM DataFrame into the MySQL database
ttm_pivoted_again.to_sql('ttm_table', engine, if_exists='replace', index=False)

print("Data loaded successfully into MySQL database!")
