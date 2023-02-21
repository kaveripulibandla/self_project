import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql.window import Window


def create_rawlayer():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(
        "C:\\self_project\\input_files\\sales_data.csv")

    df.printSchema()
    df.show(truncate=False)
    # df.write.mode("overwrite").format("csv").option("header", True)\
        # .save("C:\\self_project\\internal_files\\raw_sales_file.csv")

    df.coalesce(1).write.mode("overwrite").format('csv') \
        .option("header", True).save("C:\\self_project\\internal_files\\sales_raw_file.csv")

    # """"" # RAW_DATA HIVE TABLE """""
    # df.coalesce(1).write.mode("overwrite").saveAsTable("raw_sales_data")
    # df_log = spark.sql("select * from raw_sales_data")
    # df_log.show()
    #
    # df_log = spark.sql("select count(*) from raw_sales_data")
    # df_log.show()


if __name__ == '__main__':
    create_rawlayer()

