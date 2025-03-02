print('start')

import requests
import json
import pandas as pd
import csv
import psycopg2

from dotenv import load_dotenv # type: ignore
import os

def configure():
    load_dotenv() 
# Extraction layer
configure()
url = os.getenv('url')

querystring = {"limit":"100000"}

headers = {
	"x-rapidapi-key": os.getenv('x-rapidapi-key'),
	"x-rapidapi-host": os.getenv('x-rapidapi-host')
}

response = requests.get(url, headers=headers, params=querystring)

#print(response.json())

data = response.json()

# Save the data to a file
filename = "PropertyRecords.json"
with open(filename, "w") as file:
    json.dump(data, file, indent=4)
    
ProjectRecords_pd = pd.read_json('PropertyRecords.json')

#Coverting dictionary columns into string
def convert_dict_columns_to_string(ProjectRecords_pd: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all dictionary-type columns in a DataFrame to string format.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with dictionary columns converted to strings.
    """
    for col in ProjectRecords_pd.columns:
        # Check if the column contains dictionary entries
        if ProjectRecords_pd[col].apply(lambda x: isinstance(x, dict)).any():
            ProjectRecords_pd[col] = ProjectRecords_pd[col].apply(lambda x: str(x) if isinstance(x, dict) else x)
    return ProjectRecords_pd
ProjectRecords_pd = convert_dict_columns_to_string(ProjectRecords_pd)

# Replace Nan Values with appropraite defaults or remove row/columns as neccesary
ProjectRecords_pd.fillna({
    "assessorID": "Unknown",
    "legalDescription": "Not available",
    "squareFootage": 0,
    "subdivision": "Not available",
    "yearBuilt": 0,
    "bathrooms": 0,
    "lotSize": 0,
    "propertyType": "Unknown",
    "lastSalePrice":0,
    "lastSaleDate": "Not available",
    "features": "None",
    "taxAssessment": "Not available",
    "owner": "Unknown",
    "propertyTaxes": "Not available",
    "bedrooms": 0,
    "ownerOccupied": 0,
    "zoning": "Unknown",
    "addressLine2": "Not available",
    "formattedAddress": "Not Available",
    "county": "Not available"
    
}, inplace=True)

# Converting year built colummn from float to Integer
ProjectRecords_pd['yearBuilt'] = ProjectRecords_pd['yearBuilt'].astype(int)
ProjectRecords_pd['lastSaleDate'] = pd.to_datetime(ProjectRecords_pd['lastSaleDate'], errors='coerce')

# Create location Dimension
location_dim = ProjectRecords_pd[["city", "county", "state"]].drop_duplicates()
location_dim = location_dim.reset_index(drop=True).reset_index().rename(columns={"index": "location_id"})
location_dim["location_id"] += 1  # Start index from 1 instead of 0

# Create Property Features Dimension
description_dim = ProjectRecords_pd[["propertyType", "bathrooms", "bedrooms", "yearBuilt", "squareFootage", "lotSize"]].drop_duplicates().reset_index(drop=True)
description_dim  = description_dim.reset_index().rename(columns={"index": "description_id"})
description_dim ["description_id"] += 1  # Start index from 1 instead of 0

# Create time Dimension
time_dim = ProjectRecords_pd[["lastSaleDate"]].drop_duplicates().reset_index(drop=True)
time_dim = time_dim.reset_index().rename(columns={"index": "time_id"})
time_dim["time_id"] += 1  # Start index from 1 instead of 0
time_dim['year'] = time_dim['lastSaleDate'].dt.isocalendar().year  # year
time_dim['week'] = time_dim['lastSaleDate'].dt.isocalendar().week  # ISO week number
time_dim['month'] = time_dim['lastSaleDate'].dt.month             # Month number
time_dim['quarter'] = time_dim['lastSaleDate'].dt.quarter          # Quarter (1-4)
time_dim['day_of_week'] = time_dim['lastSaleDate'].dt.day_name()   # Full weekday name 

# Create the fact table and linking of dimension table primary key with fact table
fact_columns = ["addressLine1", "city", "lastSaleDate", "propertyType", "bathrooms", "bedrooms", "yearBuilt", "squareFootage", "lotSize", 'lastSalePrice']
fact_table = ProjectRecords_pd[fact_columns]
fact_table = fact_table.merge(time_dim[["lastSaleDate", "time_id"]], on="lastSaleDate", how="left")
fact_table = fact_table.merge(location_dim[["city", "location_id"]], on="city", how="left")
fact_table = fact_table.merge(description_dim, on=["propertyType", "bathrooms", "bedrooms", "yearBuilt", "squareFootage", "lotSize"], how="left")
fact_table .drop(columns=["lastSaleDate","city","propertyType", "bathrooms", "bedrooms", "yearBuilt", "squareFootage", "lotSize"], inplace=True)

# Saving fact and dimensions table in csv format
fact_table.to_csv("property_fact.csv", index=False)
location_dim.to_csv("location_dimension.csv", index=False)
description_dim.to_csv("description_dimension.csv", index=False)
time_dim.to_csv("time_dimension.csv", index=False)


# develop a function to connect to pgadmin
def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database = 'postgres',
        user = 'postgres',
        password = "OYE080tayo@"
    )
    return connection

conn = get_db_connection()

# Creating tables
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query =  '''CREATE SCHEMA IF NOT EXISTS zapbank;
    
                             DROP TABLE IF EXISTS zapbank.fact_table;
                             DROP TABLE IF EXISTS zapbank.location_dim;
                             DROP TABLE IF EXISTS zapbank.sales_dim;
                             DROP TABLE IF EXISTS zapbank.dates_dim;
                             DROP TABLE IF EXISTS zapbank.description_dim;
                             
                             CREATE TABLE zapbank.fact_table(
                                 addressLine1 VARCHAR(255),
                                 time_id FLOAT,
                                 location_id FLOAT,
                                 description_id FLOAT,
                                 lastSalePrice FLOAT  
                             );
                             
                             CREATE TABLE zapbank.location_dim(
                                 location_id SERIAL PRIMARY KEY,
                                 city VARCHAR(100),
                                 county VARCHAR(50),
                                 state VARCHAR(50)
                             );
                             
                             CREATE TABLE zapbank.description_dim(
                                 description_id SERIAL PRIMARY KEY,
                                 propertyType VARCHAR(50),
                                 bathrooms FLOAT,
                                 bedrooms FLOAT,
                                 yearBuilt INTEGER, 
                                 squareFootage FLOAT,
                                 lotSize FLOAT
                             ); 
                             
                             CREATE TABLE zapbank.dates_dim(
                                 time_id SERIAL PRIMARY KEY,
                                 lastSaleDate DATE,
                                 year INTEGER,
                                 week INTEGER,
                                 month FLOAT,
                                 quater FLOAT,
                                 day_of_week VARCHAR(50)   
                             );'''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    
create_tables()

# create a function to load the csv data into the database
def load_data_from_csv_to_table(csv_path,  table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r', encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                placeholders = ', '.join(['%s'] * len(row))
                query = f'INSERT INTO {table_name} VALUES({placeholders});'
                cursor.execute(query, row)
    conn.commit()
    cursor.close()
    conn.close()     
    
# fact table
fact_csv_path = r'/Users/OYETAYOADEDAMOLA/Documents/AmdariProject/Project1/property_fact.csv'
load_data_from_csv_to_table(fact_csv_path,'zapbank.fact_table')

# location dimension table
location_csv_path = r'/Users/OYETAYOADEDAMOLA/Documents/AmdariProject/Project1/location_dimension.csv'
load_data_from_csv_to_table(location_csv_path,'zapbank.location_dim')

# description dimension table
description_csv_path = r'/Users/OYETAYOADEDAMOLA/Documents/AmdariProject/Project1/description_dimension.csv'
load_data_from_csv_to_table(description_csv_path,'zapbank.description_dim')

# create a function to load the csv data into the database (specifically for sales)
# create a function to load the csv data into the database (specifically for sales)
# create a function to load the csv data into the database (specifically for sales)
def load_data_from_csv_to_sales_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r', encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                # convert empty strings (or 'Not available') in date column to None(Null in SQL)
                row = [None if (cell == "" or cell == "Not available") else cell for cell, col_name in zip(row, time_dim.columns)]
                placeholders = ', '.join(['%s'] * len(row))
                query = f'INSERT INTO {table_name} VALUES({placeholders});'
                cursor.execute(query, row)
    conn.commit()
    cursor.close()
    conn.close()



print('All Data has been loaded successfully into their respective schema and tables')