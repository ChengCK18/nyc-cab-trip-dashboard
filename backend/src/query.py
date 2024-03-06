import pandas as pd
import mysql.connector
from multiprocessing import Pool
import tqdm
from datetime import datetime


def q_get_cheapest(cursor,year_param_parsed,month_param_parsed,day_param_parsed,selectedPOLoc,
                   selectedDOLoc,selectedPassengerCount,selectedTripDistFrom,selectedTripDistTo):
    query = f"""
    SELECT DATE_FORMAT(tpep_pickup_datetime, '%Y-%m-%d %H:00:00') AS pickup_hour,AVG(total_amount) AS average_fare
    FROM yellow_cab_all
    WHERE 
        YEAR(tpep_pickup_datetime) = {year_param_parsed}
        AND MONTH(tpep_pickup_datetime) = {month_param_parsed}
        AND DAY(tpep_pickup_datetime) = {day_param_parsed}
        AND PULocationID = {selectedPOLoc}
        AND DOLocationID = {selectedDOLoc}
        {"" if selectedPassengerCount == "0" else f"AND passenger_count = {selectedPassengerCount}"}
        {"" if selectedTripDistFrom == "0" else f"AND trip_distance >= {selectedTripDistFrom}"}
        {"" if selectedTripDistTo == "0" else f"AND trip_distance <= {selectedTripDistTo}"}
    GROUP BY 
        pickup_hour
    ORDER BY 
        average_fare asc;
    """
    print(query)
    cursor.execute(query)
    results = cursor.fetchall()
    return results

def q_get_loc_data(cursor):
    query = f"""
            SELECT * FROM location_data;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results


def q_check_requested_data_inserted(cursor,year_param_parsed,month_param_parsed):
    query = f"""
            SELECT * FROM insertedData WHERE DATE_FORMAT(dataDate, '%Y-%m') = '{year_param_parsed}-{month_param_parsed}';
        """
    cursor.execute(query)
    results = cursor.fetchall()
    return results


def q_remove_overhead_3mth(cnx,cursor):
    query = f"""
            SELECT * FROM insertedData order by insertionDate asc;
        """
    cursor.execute(query)
    results = cursor.fetchall()
    if(len(results)>=3): # deletes from insertionOrder lookup table and also from main data table
        oldest_batch_date = results[0][0]
        oldest_batch_year = oldest_batch_date.year
        oldest_batch_month = oldest_batch_date.month
        query1 = f"""
            DELETE FROM insertedData WHERE dataDate = '{oldest_batch_year}-{oldest_batch_month}-01';
        """
        cursor.execute(query1)

        query2 = f"""
            DELETE FROM yellow_cab_all WHERE YEAR(tpep_pickup_datetime)={oldest_batch_year} 
            AND MONTH(tpep_pickup_datetime)={oldest_batch_month};
        """
        cursor.execute(query2)
        cnx.commit()


def q_get_trip_count(cursor,year_param_parsed,month_param_parsed,day_param_parsed):
    query= f"""
        SELECT DATE_FORMAT(tpep_pickup_datetime, '%Y-%m-%d %H:00:00') AS pickup_hour, COUNT(*) AS trip_count
        FROM yellow_cab_all
        WHERE tpep_pickup_datetime IS NOT NULL
            AND YEAR(tpep_pickup_datetime) = {year_param_parsed}
            AND MONTH(tpep_pickup_datetime) = {month_param_parsed}
            AND DAY(tpep_pickup_datetime) = {day_param_parsed}
        GROUP BY pickup_hour
        ORDER BY pickup_hour;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    return results


def q_insert_new_data(cnx,cursor,save_path,year_param_parsed,month_param_parsed):
    df = pd.read_parquet(save_path)
    num_processes = 6 

    chunk_size = len(df) // num_processes
    remainder = len(df) % num_processes
    chunks = [df[i:i+chunk_size] for i in range(0, len(df) - remainder, chunk_size)]
    if remainder:
        chunks.append(df[-remainder:])

    # # # Create a pool of processes
    with Pool(num_processes) as pool:
        # Map each chunk to the insert_rows function
        pool.map(insert_rows, chunks)


    # to add record to lookup table indicating this batch of data is in the DB
    query=f"""
        INSERT INTO insertedData (dataDate) VALUES ('{year_param_parsed}-{month_param_parsed}-01')
    """
    cursor.execute(query)
    cnx.commit()

    return True
    

 

    
def insert_rows(chunk):
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
                return None
            

    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="pw",  
        database="yellow_cab_db"
    )
    cursor = cnx.cursor()

    batch_size = 200000
    values_accumulated = []

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
        
       
        values_accumulated.append(extracted_vals)
        # Check if the accumulated values reach the batch size
        if len(values_accumulated) >= batch_size:
            # Construct the multiple value sets for the INSERT query
            values_str = ', '.join(["(" + ', '.join(f"'{item}'" if item is not None else 'NULL' for item in val) + ")" for val in values_accumulated])
            # Construct the INSERT query with multiple value sets
            insert_query = f"""
            INSERT INTO {table_name} 
            VALUES {values_str}
            """

            # Execute the INSERT query
            cursor.execute(insert_query)

            # Clear the accumulated values list
            values_accumulated = []

    # After the loop, insert any remaining accumulated values
    if values_accumulated:
        values_str = ', '.join(["(" + ', '.join(f"'{item}'" if item is not None else 'NULL' for item in val) + ")" for val in values_accumulated])
        
        insert_query = f"""
        INSERT INTO {table_name} 
        VALUES {values_str}
        """
        cursor.execute(insert_query)

    cnx.commit()
    
    cursor.close()
    cnx.close()

    
       
  



    