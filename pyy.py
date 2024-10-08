import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

url = 'https://screener.in/company/RELIANCE/consolidated/'
webpage = requests.get(url)
soup = bs(webpage.text,'html.parser')
data = soup.find('section', id="profit-loss")
if data is not None:
    tdata = data.find("table")
    if tdata is not None:
        table_data = []
        for row in tdata.find_all('tr'):
            row_data = []
            for cell in row.find_all(['th','td']):
                row_data.append(cell.text.strip())
            table_data.append(row_data)

        df_table = pd.DataFrame(table_data)
        df_table.iloc[0,0] = 'Section'
        df_table.columns = df_table.iloc[0]
        df_table = df_table.iloc[1:,:-2]
        for i in df_table.iloc[:,1:].columns:
            df_table[i] = df_table[i].str.replace(',','').str.replace('%','/100').apply(eval)
        # df_table = df_table.set_index('')
        print(df_table)

        db_host = "192.168.3.38" 
        db_name = "mydatabase"
        db_user = "anjali"
        db_password = "anjali" 
        db_port="5432"
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
        print("Data loaded to Postgres")
    else:
        print("No table found in the section")
else:
    print("No section with id 'profit-loss' found")
