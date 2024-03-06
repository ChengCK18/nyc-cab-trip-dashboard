import pandas as pd
import mysql.connector
import os

# Function to get column names and types from a DataFrame
def get_column_info(df):
    column_info = []
    for column in df.columns:
        column_type = df[column].dtype
        if pd.api.types.is_integer_dtype(column_type):
            column_info.append(f"{column} INT")
        elif pd.api.types.is_float_dtype(column_type):
            column_info.append(f"{column} FLOAT")
        elif pd.api.types.is_string_dtype(column_type):
            max_length = df[column].str.len().max()
            column_info.append(f"{column} VARCHAR({max_length})")
        else:
            column_info.append(f"{column} DATETIME")
    return column_info


# Establish connection to MySQL
cnx = mysql.connector.connect(
    host="localhost",
    user="root",
    password="pw",  # Use the password you set when starting the MySQL container
    database="yellow_cab_db"  # Specify the database you want to connect to
)

# Create a cursor
cursor = cnx.cursor()
table_name = "yellow_cab_all"

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, 'yellow_tripdata_2022-08.parquet')


df = pd.read_parquet(file_path)
column_info = get_column_info(df)

#### MAIN TABLE
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(column_info)}
    )
"""

cursor.execute(create_table_query)


#### LOOKUP TABLE
create_lookup_table_query = f"""
    CREATE TABLE insertedData (
        dataDate DATE NOT NULL PRIMARY KEY,
        insertionDate DATETIME DEFAULT CURRENT_TIMESTAMP
    );
"""
cursor.execute(create_lookup_table_query)




#### LOCATION TABLE
create_location_table_query = f"""
    CREATE TABLE location_data (
        LocationID INT PRIMARY KEY,
        Borough VARCHAR(50),
        Zone VARCHAR(50),
        service_zone VARCHAR(50)
    );
"""
cursor.execute(create_location_table_query)

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, 'taxi_zone_lookup.csv')
df = pd.read_csv(file_path)

for _, row in df.iterrows():
    insert_query = f"""
    INSERT INTO  location_data(LocationID, Borough, Zone, service_zone) 
    VALUES (%s, %s, %s, %s)
    """
    LocationID = row[0] if not pd.isna(row[0]) else 'Null'
    Borough = row[1] if not pd.isna(row[1]) else 'Null'
    Zone = row[2] if not pd.isna(row[2]) else 'Null'
    service_zone = row[3] if not pd.isna(row[3]) else 'Null'

    
    parsed = (LocationID,Borough,Zone,service_zone)
    cursor.execute(insert_query, parsed)



cnx.commit()

print("Table successfully created")
cursor.close()
cnx.close()






'''
CREATE TABLE insertedData (
    dataDate DATE PRIMARY KEY,
    insertionDate DATETIME DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO insertedData (dataDate)
VALUES ('2020-01-01');

'''
