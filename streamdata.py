import requests
import pyodbc
import pandas as pd
import time

# Power BI Streaming Dataset API endpoint
api_url = "https://api.powerbi.com/beta/a60ff60c-87ca-4db4-9c69-a216da7a6487/datasets/e9e99bca-64a5-4300-962a-4b4faa08ef86/rows?experience=power-bi&key=5Dua0npoJGFyW6aPtcAF6tirWWPCHtHmG89aARKfWM6SgrTrlt1V5J8w0%2B8LyWWFLtFky5InuYBgBrx3muNMsQ%3D%3Dss"

# SQL Server connection settings
server = 'DESKTOP-R8U2TTC\SQLEXPRESS'
database = 'live_sales'
username = 'live_user'
password = 'test123'

# Delay interval in seconds (adjust as needed)
delay_seconds = 60  # This example fetches data every 60 seconds

while True:
    try:
        # Establish a connection to the SQL Server database
        conn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")

        # SQL query to retrieve new data
        sql_query = "SELECT [SaleID], [Product], [Quantity], [Price], [Customer], [SaleDatetime] " \
            "FROM [live_sales].[dbo].[Sales] " \
            "WHERE [SaleDatetime] > DATEADD(SECOND, -1, GETDATE())"

        # Fetch new data from SQL Server into a Pandas DataFrame
        df = pd.read_sql(sql_query, conn)

        # Convert DataFrame rows to a list of dictionaries
        data = df.to_dict(orient="records")

        # Send data to the Power BI streaming dataset
        headers = {
            "Content-Type": "application/json"
        }

        response = requests.post(api_url, json=data, headers=headers)

        if response.status_code == 200:
            print("Data sent successfully to Power BI Streaming Dataset.")
        else:
            print(f"Error sending data: {response.status_code} - {response.text}")

        # Close the SQL Server connection
        conn.close()
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    # Sleep for the specified delay before fetching data again
    time.sleep(delay_seconds)
