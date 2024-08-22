import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Download and unzip it: "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"


log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file):
    df = pd.read_csv(file)
    return df

def extract_from_json(file):
    df = pd.read_json(file, lines=True)
    return df

def extract_from_xml(file):
    df = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = car.find("year_of_manufacture").text
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        df = pd.concat([df, pd.DataFrame([{"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}])], ignore_index=True)
    return df

def extract():
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data
    
def transform(data):
    data["price"] = round(data.price, 2)
    return data

def load(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as f:
        f.write(f"{timestamp}, {message} \n")

log_progress("ETL Job Started")
log_progress("Extract job Started")
extracted_data = extract()
log_progress("Extract job Ended")

log_progress("Transform job Started")
transformed_data = transform(extracted_data)
print("Transformed Data") 
print(transformed_data) 
log_progress("Transform job Ended")

log_progress("Load job Started")
load(target_file, transformed_data)
log_progress("Load job Ended")

log_progress("ETL job Ended")