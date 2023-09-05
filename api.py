import pyodbc
import pandas as pd
import requests
import adal
import json
import time
from datetime import datetime, timedelta

# Define your SQL Server connection parameters
server = 'DESKTOP-R8U2TTC\SQLEXPRESS'
database = 'live_sales'
username = 'live_user'
password = 'test123'

while True:
    # Establish a connection to the SQL Server database
    connection_string = f'DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
    connection = pyodbc.connect(connection_string)

    # Calculate the timestamp for 1 minute ago
    one_minute_ago = datetime.now() - timedelta(minutes=1)

    # SQL query to select data from the Sales table for the last 1 minute
    sql_query = f'''
    SELECT [SaleID],[Product],[Quantity],[Price],[Customer],[SaleDatetime] 
    FROM [dbo].[Sales]
    WHERE [SaleDatetime] >= ?
    '''

    # Execute the SQL query with the timestamp parameter and fetch the data into a Pandas DataFrame
    sales_data = pd.read_sql(sql_query, connection, params=[one_minute_ago])

    # Close the database connection
    connection.close()

    # Convert the DataFrame to a list of dictionaries
    data_records = sales_data.to_dict(orient='records')

    # Define your Power BI streaming dataset API endpoint
    api_endpoint = 'https://api.powerbi.com/beta/a60ff60c-87ca-4db4-9c69-a216da7a6487/datasets/e9e99bca-64a5-4300-962a-4b4faa08ef86/rows?experience=power-bi&key=5Dua0npoJGFyW6aPtcAF6tirWWPCHtHmG89aARKfWM6SgrTrlt1V5J8w0%2B8LyWWFLtFky5InuYBgBrx3muNMsQ%3D%3D'

    # Acquire an Access Token using adal
    authority_url = 'https://login.microsoftonline.com/a60ff60c-87ca-4db4-9c69-a216da7a6487'
    resource_url = 'https://analysis.windows.net/powerbi/api'

    context = adal.AuthenticationContext(authority_url)
    token = context.acquire_token_with_client_credentials(
        resource_url,
        'da497272-9baa-4a42-bad0-f48b7e3f301c',
        'FSq8Q~PzLobgKQkBWOeviFthxp1S86xFWscOOaas'
    )

    api_token = token['accessToken']

    # Convert the list of dictionaries to JSON format
    data_json = json.dumps(data_records)

    # Push data to the Power BI streaming dataset
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_token}'
    }

    response = requests.post(api_endpoint, data=data_json, headers=headers)

    if response.status_code == 200:
        print('Data pushed successfully')
    else:
        print(f'Error pushing data: {response.status_code}, {response.text}')

    time.sleep(1)
