'''
=================================================
Milestone 3

Nama  : Khoizuron Syakira
Batch : CODA RMT-023

This program is made to read (extract) the Travel & Tourism booking data with PySpark.
The result is saved to a CSV file so it can be processed by transform.py.

Features:
- read the raw CSV file into a Spark DataFrame
- create the output folder if it does not exist yet
- save the data as 1 CSV file for the transform step
=================================================
'''
import os # used to create the folder for the extract

from pyspark.sql import SparkSession #used to cerate spark session

INPUT = "/workspace/data/P2M3_khoizuron_syakira_data_raw.csv"  # location of the raw data
OUTPUT = "/workspace/data/travel/extract_result.csv"  # location of the extract result


def load_data(file_path):
    '''
    this function is made to read a CSV file into a spark DataFrame.
    
    - Parameters:
     file_path: string - location of the CSV file to read
     
    - Return
     data: Spark DataFrame - the data from the CSV file
     
    - Example of use:
     data = load_data('/workspace/data/P2M3_khoizuron_syakira_data_raw.csv')
    '''
    spark = SparkSession.builder.appName("TravelExtract").getOrCreate() # create the Spark session
    data = spark.read.csv(file_path, header=True, inferSchema=True) # read the CSV using the first row as column names, and let Spark read the data types
    
    return data


if __name__ == "__main__":
    # first I read the raw data with PySpark using the load_data function
    data = load_data(INPUT)

    # next I create the output folder so there is no error if the folder does not exist yet
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

    # the data is small (1,000 rows), so I save it as 1 CSV file using pandas
    data.toPandas().to_csv(OUTPUT, index=False)
    print(f"Saved: {OUTPUT}")

    # stop the active Spark session to free up resources
    data.sparkSession.stop()