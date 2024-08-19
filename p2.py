import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# URL to scrape
url = 'https://screener.in/company/RELIANCE/consolidated/'

# Send a GET request to the URL
webpage = requests.get(url)

# Parse the HTML content using BeautifulSoup
soup = bs(webpage.text, 'html.parser')

# Find the section with the profit-loss data
data = soup.find('section', id="profit-loss")

if data is not None:
    # Find the table within the section
    tdata = data.find("table")
    
    if tdata is not None:
        # Extract the table data
        table_data = []
        for row in tdata.find_all('tr'):
            row_data = []
            for cell in row.find_all(['th', 'td']):
                row_data.append(cell.text.strip())
            table_data.append(row_data)

        # Convert the table data to a Pandas DataFrame
        df_table = pd.DataFrame(table_data)
        df_table.iloc[0, 0] = 'Section'
        df_table.columns = df_table.iloc[0]
        df_table = df_table.iloc[1:, :-2]
        
        # Convert the numeric columns to numeric values
        for i in df_table.iloc[:, 1:].columns:
            df_table[i] = df_table[i].str.replace(',', '').str.replace('%', '/100').apply(eval)

        # Define the database connection parameters
        db_host = "192.168.3.38"
        db_name = "mydatabase"
        db_user = "anjali"
        db_password = "anjali"
        db_port = "5432"

        # Create a database engine
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

        # Load the data into the Postgres database
        df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
        print("Data loaded to Postgres")
    else:
        print("No table found in the section")
else:
    print("No section with id 'profit-loss' found")
