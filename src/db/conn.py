import pyodbc
import pandas as pd
import os 
from sqlalchemy import create_engine
import pyodbc
import pandas as pd

def getData():
    server = "nexumuat.database.windows.net"
    database = "nexum_base_demo_3_6_etl"
    username = "nexum"
    password = r"ff=E(;A85u7oJK+4"

    # Connection string
    connection_string = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={{{password}}};'
    )

    conn = None
    cursor = None

    try:
        # Establishing a connection to the SQL Server
        conn = pyodbc.connect(connection_string)

        # Instantiate a cursor
        cursor = conn.cursor()

        # Execute the second query to get id, idcase, idcodeplabel, idcodeworkstage, amount, dtcreate
        cursor.execute(
        """
        SELECT
            FORMAT([Transaction Creation Date], 'yyyy-MM') AS payment_month,
            SUM([Transaction amount]) AS total_amount
            , NEXUM_Tennant
        FROM
            TRANS_PAY_ETL
        WHERE
            YEAR([Transaction Creation Date]) BETWEEN 2014 AND 2023
        GROUP BY
            FORMAT([Transaction Creation Date], 'yyyy-MM'), NEXUM_Tennant
        ORDER BY
            payment_month;
        """
        )
        rows = cursor.fetchall()
        # Fetch all rows from the result set of the second query
        df = pd.DataFrame([tuple(row) for row in rows], columns=[col[0] for col in cursor.description])
        return df

    except pyodbc.Error as ex:
        print(f"Error: {ex}")
        return None
    finally:
        # Close the cursor in a finally block if it is still open
        if cursor:
            cursor.close()

def writeToDatabase(df, table_name):
    try:
        print("hii")
        server = "nexumuat.database.windows.net"
        database = "nexum_base_demo_3_6_etl"
        username = "nexum"
        password = "ff=E(;A85u7oJK+4"
        driver = "ODBC+Driver+17+for+SQL+Server"
        
        print(df.head(5))
        
        # Create the engine
        engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}")
        
        # Check table_name to determine which table to insert into
        if table_name == "Nexum_PayAmount_Prediction":
            df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
        elif table_name == "Nexum_PayAmount_Forecast":
            df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
        else:
            print("Invalid table name.")
            return "Invalid table name."
        
        return "Processed successfully"

    except pyodbc.Error as e:
        print(e)

# df = query_aps_trans_data()
# if df is not None:
#     print(df.head(5))
