print('start')

import requests
import json
import pandas as pd
import csv
import psycopg2

url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

querystring = {"limit":"100000"}

headers = {
	"x-rapidapi-key": "44f9cb2ad8msh6f3d4d207145829p1f2707jsn317690134bb4",
	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
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

# Create the fact table
fact_columns = ["addressLine1", "city", "state", "zipCode", "formattedAddress", "squareFootage", "yearBuilt", "bathrooms",
               "bedrooms", "lotSize", "longitude", "latitude"]
fact_table = ProjectRecords_pd[fact_columns]

# Create location Dimension
location_dim = ProjectRecords_pd[["addressLine1", "city", "state", "zipCode","longitude", "latitude"]].drop_duplicates()
location_dim.index.name = "location_id"

# Create sale Dimension
sales_dim = ProjectRecords_pd[['lastSalePrice', "lastSaleDate"]].drop_duplicates().reset_index(drop=True)
sales_dim.index.name = "sales_id"

# Create Property Features Dimension
features_dim = ProjectRecords_pd[["features", "propertyType", "zoning"]].drop_duplicates().reset_index(drop=True)
features_dim.index.name = "features_id"

# Saving fact and dimensions table in csv format
fact_table.to_csv("property_fact.csv", index=False)
location_dim.to_csv("location_dimension.csv", index=True)
sales_dim.to_csv("sales_dimension.csv", index=True)
features_dim.to_csv("features_dimension.csv", index=True)

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
                             DROP TABLE IF EXISTS zapbank.features_dim;
                             
                             CREATE TABLE zapbank.fact_table(
                                 addressLine1 VARCHAR(255),
                                 city VARCHAR(100),
                                 state VARCHAR(50),
                                 zipCode INTEGER,
                                 formattedAddredd VARCHAR(255),
                                 squareFootage FLOAT,
                                 yearBuilt FLOAT,
                                 bathrooms FLOAT,
                                 bedrooms FLOAT,
                                 propertyType VARCHAR(100),
                                 longitude FLOAT,
                                 latitude FLOAT
                             );
                             
                             CREATE TABLE zapbank.location_dim(
                                 location_id SERIAL PRIMARY KEY,
                                 addressLine1 VARCHAR(255),
                                 city VARCHAR(100),
                                 state VARCHAR(50),
                                 zipCode INTEGER,
                                 longitude FLOAT,
                                 latitude FLOAT
                             );
                             
                             CREATE TABLE zapbank.sales_dim(
                                 sales_id SERIAL PRIMARY KEY,
                                 lastSalePrice FLOAT,
                                 lastSaleDate DATE
                             ); 
                             
                             CREATE TABLE zapbank.features_dim(
                                 features_id SERIAL PRIMARY KEY,
                                 features TEXT,
                                 propertyType VARCHAR(100),
                                 zoning VARCHAR(100)
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

# feature dimension table
feature_csv_path = r'/Users/OYETAYOADEDAMOLA/Documents/AmdariProject/Project1/features_dimension.csv'
load_data_from_csv_to_table(feature_csv_path,'zapbank.features_dim')

# create a function to load the csv data into the database (specifically for sales)
def load_data_from_csv_to_sales_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r', encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                # convert empty strings (or 'Not available') in date column to None(Null in SQL)
                row = [None if (cell == "" or cell == "Not available") and col_name == 'lastSaleDate' else cell for cell, col_name in zip(row, sale_dim_columns)]
                placeholders = ', '.join(['%s'] * len(row))
                query = f'INSERT INTO {table_name} VALUES({placeholders});'
                cursor.execute(query, row)
    conn.commit()
    cursor.close()
    conn.close()
    
sale_dim_columns = ['sales_id', 'lastSalePrice', 'lastSaleDate']

# sales dimension table
sales_csv_path = r'/Users/OYETAYOADEDAMOLA/Documents/AmdariProject/Project1/sales_dimension.csv'
load_data_from_csv_to_sales_table(sales_csv_path,'zapbank.sales_dim')

print('All Data has been loaded successfully into their respective schema and tables')