# import pandas as pd
# import mysql.connector
# import tqdm

# # Read Parquet file into DataFrame
# df = pd.read_parquet('/home/primis/Desktop/endpoint/data/yellow_tripdata_2020-01.parquet')

# # Connect to MySQL database
# cnx = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="pw",
#     database="yellow_cab_db"  
# )

# # Create a cursor
# cursor = cnx.cursor()

# # Define table name
# table_name = "yellow_cab_all"
# count=0
# # Insert rows into the table

# for _, row in tqdm.tqdm(df.iterrows(), total=df.shape[0]):
#     # data_values = [str(val) if not isinstance(val, datetime) else val.strftime("%Y-%m-%d %H:%M:%S") for val in result]
#     data_values = ','.join([f'\'{val}\'' for val in row]) # to parse array data into mysql format with each val enclosed in '' and seperated by comma ,

#     insert_query = f"""
#     INSERT INTO {table_name} 
#     VALUES ({data_values})
#     """
#     # Execute insert query

#     cursor.execute(insert_query)
   
    

# # Commit changes
# cnx.commit()

# # Close cursor and connection
# cursor.close()
# cnx.close()



import pandas as pd
import mysql.connector
from multiprocessing import Pool
import tqdm
from datetime import datetime


def parse_with_default(value, default,col):

    if(pd.isna(value) or pd.isnull(value)):
        return None
    try:
        return type(default)(value)
    except (ValueError, TypeError):
        try:
            pd.Timestamp(value)
            return value
        except ValueError:
            print(col,'=>',value)
            return None
        

def insert_rows(chunk):
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="pw",  
        database="yellow_cab_db"
    )
    cursor = cnx.cursor()

    table_name = "yellow_cab_all"
    for _, row in tqdm.tqdm(chunk.iterrows(),total=chunk.shape[0]):

        # Assigning variables
        VendorID = parse_with_default(row.iloc[0], 0, 'VendorID')
        tpep_pickup_datetime = parse_with_default(row.iloc[1], datetime.now(), 'tpep_pickup_datetime')
        tpep_dropoff_datetime = parse_with_default(row.iloc[2], datetime.now(), 'tpep_dropoff_datetime')
        passenger_count = parse_with_default(row.iloc[3], 0, 'passenger_count')
        trip_distance = parse_with_default(row.iloc[4], 0.0, 'trip_distance')
        RatecodeID = parse_with_default(row.iloc[5], 0, 'RatecodeID')
        store_and_fwd_flag = parse_with_default(row.iloc[6], 'N', 'store_and_fwd_flag')
        PULocationID = parse_with_default(row.iloc[7], 0, 'PULocationID')
        DOLocationID = parse_with_default(row.iloc[8], 0, 'DOLocationID')
        payment_type = parse_with_default(row.iloc[9], 0, 'payment_type')
        fare_amount = parse_with_default(row.iloc[10], 0.0, 'fare_amount')
        extra = parse_with_default(row.iloc[11], 0.0, 'extra')
        mta_tax = parse_with_default(row.iloc[12], 0.0, 'mta_tax')
        tip_amount = parse_with_default(row.iloc[13], 0.0, 'tip_amount')
        tolls_amount = parse_with_default(row.iloc[14], 0.0, 'tolls_amount')
        improvement_surcharge = parse_with_default(row.iloc[15], 0.0, 'improvement_surcharge')
        total_amount = parse_with_default(row.iloc[16], 0.0, 'total_amount')
        congestion_surcharge = parse_with_default(row.iloc[17], 0.0, 'congestion_surcharge')
        airport_fee = parse_with_default(row.iloc[18], 0.0, 'airport_fee')

        extracted_vals = [VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,
                          passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,
                          PULocationID,DOLocationID,payment_type,fare_amount,extra,
                          mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,
                          congestion_surcharge,airport_fee]
        
        data_values = ','.join([f"'{val}'" if val is not None else 'NULL' for val in extracted_vals])
       
        insert_query = f"""
        INSERT INTO {table_name} 
        VALUES ({data_values})
        """
        cursor.execute(insert_query)
        
        

    cnx.commit()

    cursor.close()
    cnx.close()


df = pd.read_parquet('./data/yellow_tripdata_2020-01.parquet')
num_processes = 6 

# chunk_size = len(df) // num_processes
# remainder = len(df) % num_processes
# chunks = [df[i:i+chunk_size] for i in range(0, len(df) - remainder, chunk_size)]
# if remainder:
#     chunks.append(df[-remainder:])


# # # Create a pool of processes
# with Pool(num_processes) as pool:
#     # Map each chunk to the insert_rows function
#     pool.map(insert_rows, chunks)
    

print("Data insertion completed.")



