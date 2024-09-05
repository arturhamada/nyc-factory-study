from pyspark.sql.functions import current_date, udf, col, unix_timestamp
from pyspark.sql.types import StringType
from utils.utils import init_spark_session, list_files



def main():

    spark = init_spark_session("read")


    file_fhvhv = "./storage/fhvhv/2022/*.parquet"
    list_files(spark, file_fhvhv)
    df_fhvhv = spark.read.parquet(file_fhvhv)

    df_fhvhv.printSchema()

    # storage = "./storage/rides/parquet/rides"
    # df_read = spark.read.format("parquet").load(storage)
    # df_read.show()





if __name__ == "__main__":
    main()
