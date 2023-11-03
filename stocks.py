import pyodbc
from pyspark.sql import SparkSession
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Define your database connection parameters
server = 'Localhost'
database = 'Broker_Config'

# Use trusted connection (Windows authentication) to fetch ISINs using Pyodbc
connection = pyodbc.connect(f'Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection=yes;')

# Define the SQL query to fetch ISINs and corresponding tickers
sql_query = """
SELECT DISTINCT TOP 10 ISINCode, TickerSymbol
FROM Instruments
WHERE TickerSymbol NOT LIKE '%LOCK%'
AND listed = 1
AND Displayname LIKE '%Inc%'
AND ISINCode IS NOT NULL
"""

# Execute the query using Pyodbc
cursor = connection.cursor()
cursor.execute(sql_query)

# Fetch the ISINs and corresponding tickers and store them in a dictionary
isin_to_ticker = {row.ISINCode: row.TickerSymbol for row in cursor.fetchall()}

# Close the Pyodbc connection
connection.close()

# Initialize a Spark session
spark = SparkSession.builder.appName("Stocks").getOrCreate()

# Define the start date and end date as the current date and the previous day
current_date = datetime.now()
end_date = current_date - timedelta(days=1)
start_date = "2023-10-26"  # You can customize the start date if needed

# Initialize an empty Spark DataFrame with a schema to match the expected data
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", IntegerType(), True),
    StructField("Adj Close", DoubleType(), True),
    StructField("TickerSymbol", StringType(), True)
])

combined_data = spark.createDataFrame([], schema)

# Iterate over each unique ISIN and download data using YFinance
for isin, ticker in isin_to_ticker.items():
    try:
        stock_data = yf.download(ticker, start=start_date, end=end_date)

        # Check if 'Date' column exists in stock_data
        if 'Date' not in stock_data.columns:
            # If 'Date' is not present, reset the index to obtain 'Date' as a column
            stock_data = stock_data.reset_index()

        stock_data['TickerSymbol'] = ticker  # Add a column for the ticker symbol

        # Convert the Pandas DataFrame to a Spark DataFrame
        stock_data_spark = spark.createDataFrame(stock_data)

        # Append the Spark DataFrame to the combined_data Spark DataFrame
        combined_data = combined_data.union(stock_data_spark)
        print(f"Data for ISIN: {isin} and Ticker: {ticker} downloaded and appended.")
    except Exception as e:
        print(f"Failed to download data for ISIN: {isin} and Ticker: {ticker}. Error: {e}")

# Save the combined data to a CSV file
combined_data.toPandas().to_csv("stock_prices_data.csv")

print("Data update complete.")
