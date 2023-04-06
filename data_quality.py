import pyspark.sql.functions as F
import configparser
import os
import logging
from pathlib import Path
from pyspark.sql import SparkSession



# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS configuration
config = configparser.ConfigParser()
config.read('config.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

# data processing functions
def create_spark_session():
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    return spark


class Data_Quality_Check:
    """
    Validate and checks the model and data.
    """
def check_table_records(spark, fname):
    fname = Path('output')
    for file_dir in fname.iterdir():
        if file_dir.is_dir():
            path = str(file_dir)
            df = spark.read.parquet(path)
            record_num = df.count()
            if record_num <= 0:
                raise ValueError("This table is empty!")
            else:
                logging.info("Table: " + path.split('/')[-1] + f" is not empty: total {record_num} records.")
    logging.info("Checking Tables Records Passed Successfully..No Empty Table Exists")

                


def check_fact_table(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_fact_immi')
    check = df.filter("cicid is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_fact_immi has no null values in PK column")

def check_dim_city_pop(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_dim_city_pop')
    check = df.filter("city is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_dim_city_pop has no null values in PK column")
        
def check_dim_city_stats(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_dim_city_stats')
    check = df.filter("city is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_dim_city_stats has no null values in PK column")
        
def check_dim_ident(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_dim_ident')
    check = df.filter("cicid is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_dim_ident has no null values in PK column")
        
def check_dim_flight(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_dim_flight')
    check = df.filter("flight_num is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_dim_flight has no null values in PK column")
        
def check_country_code(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_country_code')
    check = df.filter("code is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_country_code has no null values in PK column")
        
def check_city_code(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_city_code')
    check = df.filter("code is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_city_code has no null values in PK column")
        
def check_state_code(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_state_code')
    check = df.filter("code is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_state_code has no null values in PK column")
        
def check_visa_code(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_visa_code')
    check = df.filter("code is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_visa_code has no null values in PK column")
        
def check_transportation(spark, output_data):
    df = spark.read.parquet(output_data+ '/df_transportation')
    check = df.filter("code is null").count()
    if check != 0:
        raise ValueError("This table has null values in PK column")
    else:
        logging.info("Table df_transportation has no null values in PK column")
        logging.info("Checking Tables for Null Values Passed Successfully..No Null Values in PK Column Exist")

            
            


def main():
    spark = create_spark_session()
    input_data = SOURCE_S3_BUCKET
    output_data = 'output'
    fname = Path('output')
    
    check_table_records(spark, fname)
    check_fact_table(spark, output_data)
    check_dim_city_pop(spark, output_data)
    check_dim_city_stats(spark, output_data)
    check_dim_ident(spark, output_data)
    check_dim_flight(spark, output_data)
    check_country_code(spark, output_data)
    check_city_code(spark, output_data)
    check_state_code(spark, output_data)
    check_visa_code(spark, output_data)
    check_transportation(spark, output_data)
    logging.info("Data Quality Check Passed SUCCESSFULLY!! ")
    
if __name__ == "__main__":
    main()