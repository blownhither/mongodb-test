"""
(This is a file-level docstring.)
This file provides ETL (Extract Transform Load) functions for the NYC airbnb and NYC Taxi datasets.
"""
import csv
from time import sleep
from pymongo import MongoClient, TEXT, GEOSPHERE
from datetime import datetime


def loadAirbnb(file):
    """ Extracts the airbnb csv file into memory, Transforms certain fields, and Loads result into MongoDb.
        Creates TEXT index on 'name' and 'neighbourhood'.
        Creates GEOSPHERE index on 'location'. 
    Args:
        file: location of the airbnb csv file.
    """
    arr = []
    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row['price'] = int(row['price'])
            row['number_of_reviews'] = int(row['number_of_reviews'])
            row['latitude'] = float(row['latitude'])
            row['longitude'] = float(row['longitude'])
            row['location'] = {'type': 'Point', 'coordinates': [
                row['longitude'], row['latitude']]}
            del row['latitude']
            del row['longitude']
            arr.append(row)

    inserted_ids = db.airbnb.insert_many(arr).inserted_ids
    db.airbnb.create_index(
        [('name', TEXT), ('neighbourhood', TEXT)], default_language='english')
    db.airbnb.create_index([('location', GEOSPHERE)])

    print("{} Airbnb docs inserted".format(len(inserted_ids)))
    print("Text index created for airbnb")
    print("Geosphere index created for airbnb")


def loadTaxi(file):
    """ Extracts the taxi csv file into memory, Transforms certain fields, and Loads result into MongoDb.
    Args:
        file: location of the taxi csv file.
    """
    arr = []
    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row['fare_amount'] = float(row['fare_amount'])
            row['pickup_longitude'] = float(row['pickup_longitude'])
            row['pickup_latitude'] = float(row['pickup_latitude'])
            row['dropoff_longitude'] = float(row['dropoff_longitude'])
            row['dropoff_latitude'] = float(row['dropoff_latitude'])
            row['pickup_datetime'] = datetime.strptime(
                row['pickup_datetime'], '%Y-%m-%d %H:%M:%S %Z')
            arr.append(row)

    inserted_ids = db.taxi.insert_many(arr).inserted_ids
    print("{} taxi docs inserted".format(len(inserted_ids)))


# TODO: Create index before insert
if __name__ == "__main__":
    db = MongoClient().test
    loadAirbnb('AB_NYC_2019.csv')
    loadTaxi('TAXI_NYC_2019.csv')
