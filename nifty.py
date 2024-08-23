from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2

# Set up Selenium WebDriver
driver = webdriver.Chrome()  # Replace with your preferred browser

# Initialize empty DataFrame
df = pd.DataFrame()

# Iterate through pages
for page in range(1, 3):  # Adjust the range to include all pages
    url = f"https://www.screener.in/company/NIFTY/?page={page}"
    driver.get(url)
    
    # Wait for the table to load
    table = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "table.data-table"))
    )
    
    # Get HTML content of the table
    table_html = table.get_attribute('outerHTML')
    
    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(table_html, 'html.parser')
    
    # Extract column names
    column_names = [th.text.strip() for th in soup.find_all('th')][:11]  # Limit to 11 columns
    
    # Extract rows
    rows = []
    for row in soup.find_all('tr')[1:]:
        row_data = [td.text.strip() for td in row.find_all('td')]
        # Ensure the row has the same number of columns as column_names
        if len(row_data) != len(column_names):
            print(f"Skipping row with {len(row_data)} columns")
            continue
        rows.append(row_data)
    
    # Check if rows is not empty
    if rows:
        # Create DataFrame
        page_df = pd.DataFrame(rows, columns=column_names)
        df = pd.concat([df, page_df])

# Clean and format data
for col in df.columns:
    if 'Rs.' in col or 'Cr.' in col:
        df[col] = df[col].apply(lambda x: x.replace(',', '')).astype(float)

# Database connection parameters
db_host = "192.168.3.38"
db_name = "dbc"
db_user = "pass"
db_password = "pass"
db_port = "5432"

# Establish database connection
conn = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_password,
    port=db_port
)

# Create a cursor object
cur = conn.cursor()

# Create table if not exists
cur.execute("""
    CREATE TABLE IF NOT EXISTS nifty_companies (
        company_name VARCHAR(255),
        industry VARCHAR(255),
        market_cap VARCHAR(255),
        price VARCHAR(255),
        change VARCHAR(255),
        volume VARCHAR(255),
        turnover VARCHAR(255),
        p_e VARCHAR(255),
        p_b VARCHAR(255),
        div_yield VARCHAR(255),
        roe VARCHAR(255),
        roce VARCHAR(255)
    );
""")

# Insert data into the table
for index, row in df.iterrows():
    cur.execute("""
        INSERT INTO nifty_companies (
            company_name, industry, market_cap, price, change, volume, turnover, p_e, p_b, div_yield, roe, roce
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """, tuple(row))

# Commit changes
conn.commit()

# Close cursor and connection
cur.close()
conn.close()

# Close Selenium WebDriver
driver.quit()

print("Data inserted into PostgreSQL database successfully!")
