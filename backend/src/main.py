# from selenium import webdriver
# from selenium.webdriver.common.by import By
# driver = webdriver.Chrome()

# # URL of the webpage you want to scrape
# url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
# # Open the webpage
# driver.get(url)
# year_of_interest = "2020"
# main_year_tab = driver.find_element(By.ID, "faq"+year_of_interest)
# print(main_year_tab.text)
# main_year_tab.click()
# # Close the webdriver
# driver.quit()


# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-07.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2016-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet


import pandas as pd
import mysql.connector
from flask import Flask,request,jsonify
from flask_cors import CORS, cross_origin
from util import u_download_file
from query import q_check_requested_data_inserted,q_remove_overhead_3mth,q_insert_new_data,q_get_trip_count,q_get_loc_data,q_get_cheapest

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


table_name = "yellow_cab_all"
db_name = "yellow_cab_db"
cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="pw",  
        database=db_name
    )
cursor = cnx.cursor()


@app.route('/api/trips', methods=['GET'])
@cross_origin()
def trips_endpoint():
    year_param = request.args.get("year")
    month_param = request.args.get("month")
    day_param = request.args.get("day")
    operation = request.args.get("operation")

    try:
        year_param = int(year_param)
        month_param = int(month_param)
        day_param = int(day_param)
    except:
        data = {'message': 'Year, month, and day required and needs to be in number format'}
        response = jsonify(data)
        # Set the status code
        response.status_code = 400 #Bad request

        return response

    year_param_parsed = '{:04d}'.format(year_param)
    month_param_parsed = '{:02d}'.format(month_param)
    day_param_parsed = '{:02d}'.format(day_param)


    results = q_check_requested_data_inserted(cursor,year_param_parsed,month_param_parsed)
    # Locking mechanism here to wait for pending transac
    # transaction mechanism
    # critical region with manipulation of data.
    
    if(len(results)<=0): # data not found in sql db, would need to insert new data into DB
        q_remove_overhead_3mth(cnx,cursor) # check and delete overhead 3 mths
        print("Overhead removal done!")
        save_path = u_download_file(year_param_parsed,month_param_parsed)
        if save_path is None:
            data = {'message': f'Data is not available for requested date. {year_param_parsed}-{month_param_parsed}'}
            response = jsonify(data)
            response.status_code = 400
            return response
        
        print("Download done!")
        ret = q_insert_new_data(cnx,cursor,save_path,year_param_parsed,month_param_parsed)

        print("Insertion done!")

        if(not ret):
            data = {'message': 'something went wrong'}
            response = jsonify(data)
            response.status_code = 500
            return response


    if(operation == "q_get_trip_count"):
        results = q_get_trip_count(cursor,year_param_parsed,month_param_parsed,day_param_parsed)
        results_dict = {entry[0]: entry[1] for entry in results}
        response = jsonify(results_dict)
        # Set the status code
        response.status_code = 200
        return response
    
    elif(operation == "q_get_cheapest"):
        selectedPOLoc = request.args.get("POloc")
        selectedDOLoc = request.args.get("DOloc")
        selectedPassengerCount = request.args.get("passengerCount")
        selectedTripDistFrom = request.args.get("tripDistFrom")
        selectedTripDistTo = request.args.get("tripDistTo")

        print("selectedTripDistFrom => ",selectedTripDistFrom)
        print("selectedTripDistTo => ",selectedTripDistTo)
        results = q_get_cheapest(cursor,year_param_parsed,month_param_parsed,day_param_parsed,
                                 selectedPOLoc,selectedDOLoc,selectedPassengerCount,selectedTripDistFrom,selectedTripDistTo)
        results_dict = {entry[0]: entry[1] for entry in results}
        response = jsonify(results_dict)
        response.status_code = 200
        return response

 
@app.route('/api/locations', methods=['GET'])
@cross_origin()
def location_endpoint():
    operation = request.args.get("operation")
    if(operation == "getLocation"):
        results = q_get_loc_data(cursor)
        results_dict = {entry[0]: {
        "Borough":entry[1],
        "Zone":entry[2],
        "service_zone":entry[3]
        } for entry in results}
        response = jsonify(results_dict)
        response.status_code = 200 
        return response




# Run the Flask application
if __name__ == '__main__':
    app.run(debug=True)



# to extract data based on specific year,month,day
# SELECT * FROM yellow_cab_all WHERE DATE(tpep_pickup_datetime) = '2020-01-01';
# SELECT count(*) FROM yellow_cab_all WHERE DATE_FORMAT(tpep_pickup_datetime, '%Y-%m') = '2020-01';
    



    

