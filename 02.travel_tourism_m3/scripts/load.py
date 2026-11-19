'''
=================================================
Milestone 3

Name  : Khoizuron Syakira
Batch : CODA RMT-023

This program is made to save (load) the transformed Travel & Tourism booking data
to MongoDB Atlas with pymongo. The connection string is taken from the .env file
(MONGO_URI), so it is not written in the code.

Features:
- read the transformed CSV file
- change the empty rating to null, so it is saved correctly in MongoDB
- empty the collection first, so the data does not pile up in each scheduled run
- save every row as a document in the travel_bookings collection
=================================================
'''

import os  # to read MONGO_URI from the environment
from datetime import datetime  # to change text to datetime, because MongoDB cannot save a plain date

from pyspark.sql import SparkSession  # to create the Spark session and read the CSV
from pymongo import MongoClient  # to connect to MongoDB

INPUT = "/workspace/data/travel/transform_result.csv"  # result from transform.py

def get_collection():
    '''
    this function is made to create a connection to MongoDB Atlas.

    - Return
     collection: pymongo Collection - the travel_bookings collection in the milestone3 database

    - Example of use:
     collection = get_collection()
    '''
    client = MongoClient(os.environ["MONGO_URI"])  # to open the connection using the connection string from .env
    return client["milestone3"]["travel_bookings"]  # choose the database and collection

def load(data, collection):
    '''
    This function is made to save a Spark DataFrame to a MongoDB collection.

    Parameters:
     data: Spark DataFrame - data from the transform step
     collection: pymongo Collection - the target collection

    Example of use:
     load(data, get_collection())
    '''
    documents = []  # an empty list to keep all documents

    # I take every row from the Spark DataFrame and change it into a dictionary, because MongoDB saves data as documents
    for row in data.collect():
        document = row.asDict()

        # I change the date columns to datetime, because MongoDB cannot save a plain date
        for name in ["booking_date", "travel_date", "return_date"]:
            value = document[name]
            if value is not None:
                document[name] = datetime.strptime(str(value)[:10], "%Y-%m-%d")

        documents.append(document)

    # I empty the collection first because the DAG runs 3 times every Saturday, so the data does not pile up
    collection.delete_many({})
    collection.insert_many(documents)
    print(f"{len(documents)} documents saved to MongoDB")

if __name__ == "__main__":
    # I read the transform result with PySpark, then save it to MongoDB
    spark = SparkSession.builder.appName("TravelLoad").getOrCreate()
    data = spark.read.csv(INPUT, header=True, inferSchema=True)
    load(data, get_collection())
    spark.stop()