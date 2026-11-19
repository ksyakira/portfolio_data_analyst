'''
=================================================
Milestone 3

Name  : Khoizuron Syakira
Batch : CODA RMT-023

This program is made to clean and process (transform) the Travel & Tourism booking data
with PySpark, based on the conclusions from the data exploration in the notebook.
The data is about travel bookings: customers, destinations, hotels, payments, and trip cost.

Features:
- remove the private column (Customer_Name)
- remove extra spaces, duplicates, and empty values (as safety steps)
- change Customer_Rating value 0 to empty (null)
- change the date columns to date type
- add the column cost_per_traveller
- change all column names to lowercase snake_case
=================================================
'''

from pyspark.sql import SparkSession  # used to create the Spark session
from pyspark.sql import functions as F  # used for PySpark column functions

INPUT = "/workspace/data/travel/extract_result.csv"  # result from extract.py
OUTPUT = "/workspace/data/travel/transform_result.csv"  # result of the transform


def transform(data):
    '''
    this function is made to clean and process the booking data.
    
    - Parameters:
     data: Spark DataFrame - raw data from the extract step
     
    - Return
     data: Spark DataFrame - clean data that is ready to be saved to MongoDB
     
    - Example of use:
     data_clean = transform(data)
    '''
    
    # I remove the customer name column because it is personal data and not needed for the analysis
    data = data.drop("Customer_Name")

    # I remove extra spaces at the start and end of every text column, as a safety step because the EDA shows there are none
    for col_name, col_type in data.dtypes:
        if col_type == "string":
            data = data.withColumn(col_name, F.trim(F.col(col_name)))
    
    # After that, I remove duplicates and missing values as a safety step, even though the EDA shows both are 0
    data = data.dropDuplicates()
    data = data.dropna()
    
    # Rating 0 means the booking is not rated yet (cancelled or not travelling yet), so I change it to empty so the average rating does not go down
    data = data.withColumn("Customer_Rating", F.when(F.col("Customer_Rating") == 0, None).otherwise(F.col("Customer_Rating")))
    
    # I change the date columns to date type so they can be used for calculations
    data = data.withColumn("Booking_Date", F.to_date(F.col("Booking_Date")))
    data = data.withColumn("Travel_Date", F.to_date(F.col("Travel_Date")))
    data = data.withColumn("Return_Date", F.to_date(F.col("Return_Date")))
    
    # I make a new column named cost per traveller, so the cost of different trips can be compared fairly to be analysis
    data = data.withColumn("Cost_Per_Traveller", F.round(F.col("Total_Trip_Cost") / F.col("Number_of_Travellers"), 2))

    # Lastly, I change all the column names to lowercase with snake_case so they are easy to use in MongoDB
    data = data.toDF(*[c.strip().lower().replace(" ", "_") for c in data.columns])

    return data


if __name__ == "__main__":
    # First, I create the Spark session and read the extract result
    spark = SparkSession.builder.appName("TravelTransform").getOrCreate()
    data = spark.read.csv(INPUT, header=True, inferSchema=True)

    # The data is cleaned with the transform function, then I show the first 5 rows to check it
    result = transform(data)
    result.show(5)

    # The data is small, so I save it as 1 CSV file using pandas
    result.toPandas().to_csv(OUTPUT, index=False)
    print(f"Saved: {OUTPUT}")
    spark.stop()