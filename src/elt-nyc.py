"""
executing job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-nyc.py
"""

from pyspark.sql.functions import current_date, col, unix_timestamp

from utils.utils import init_spark_session, list_files
from utils.transformers import hvfhs_license_num




def main():

    spark = init_spark_session("elt-rides-fhvhv-py-strawberry-owshq")

    file_fhvhv = "./storage/fhvhv/2022/*.parquet"
    list_files(spark, file_fhvhv)

    fhvhv_cols = [
        "hvfhs_license_num", "PULocationID", "DOLocationID",
        "request_datetime", "pickup_datetime", "dropoff_datetime",
        "trip_miles", "trip_time", "base_passenger_fare", "tolls",
        "bcf", "sales_tax", "congestion_surcharge", "tips"
    ]

    df_fhvhv = spark.read.parquet(file_fhvhv).select(*fhvhv_cols)
    print(f"number of partitions: {df_fhvhv.rdd.getNumPartitions()}")


    df_rides = spark.sql("""
        SELECT request_datetime,
               pickup_datetime,
               dropoff_datetime,
               trip_miles,
               trip_time,
               base_passenger_fare,
               tolls,
               bcf,
               sales_tax,
               congestion_surcharge,
               tips
        FROM hvfhs
    """)

    df_rides = df_rides.withColumn("ingestion_timestamp", current_date())
    df_rides = df_rides.withColumn("time_taken_seconds", unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime")))
    df_rides = df_rides.withColumn("time_taken_minutes", col("time_taken_seconds") / 60)
    df_rides = df_rides.withColumn("time_taken_hours", col("time_taken_seconds") / 3600)

    df_rides.createOrReplaceTempView("rides")


    storage = "./storage/rides/delta/"

    df_rides.write.format("delta").mode("append").partitionBy("ingestion_timestamp").save(storage + "rides")



if __name__ == "__main__":
    main()
