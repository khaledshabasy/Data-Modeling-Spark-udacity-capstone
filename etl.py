import configparser
import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.types import IntegerType



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



def sas_to_date_type(date):
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
sas_to_date_type_udf = F.udf(sas_to_date_type, DateType())


def rename_columns(table, new_columns):
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table

def sas_labels_descr_parser(source_file, value, cols):
    file_string = ''
    
    with open(source_file) as f:
        file_string = f.read()
    
    file_string = file_string[file_string.index(value):]
    file_string = file_string[:file_string.index(';')]
    
    line_list = file_string.split('\n')[1:]
    codes = []
    values = []
    
    for line in line_list:
        
        if '=' in line:
            code, val = line.split('=')
            code = code.strip()
            val = val.strip()

            if code[0] == "'":
                code = code[1:-1]

            if val[0] == "'":
                val = val[1:-1]

        codes.append(code)
        values.append(val)
        
            
    return pd.DataFrame(list(zip(codes,values)), columns=cols)


def process_immigration_data(spark, input_data, output_data):
    """Process immigration data to get df_fact_immi, 
    df_dim_ident and df_dim_flight tables
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """

    logging.info("Start processing immigration")
    
    # read immigration data file
    immi_path = os.path.join(input_data + '/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    df_immi = spark.read.format('com.github.saurfang.sas.spark').load(immi_path)

    logging.info("Start processing fact_immigration")
    # extract columns to create df_fact_immi table
    df_fact_immi = df_immi.select('cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'i94addr',\
                                 'arrdate', 'depdate', 'i94mode', 'i94visa', 'fltno').distinct()\
                         .withColumn("immigration_id", F.monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cicid', 'year', 'month', 'country_code_cit', 'country_code_res', 'city_code',\
                   'state_code', 'arrival_date', 'departure_date', 'transportation', 'visa', 'flight_num']
    df_fact_immi = rename_columns(df_fact_immi, new_columns)

    df_fact_immi = df_fact_immi.withColumn('arrival_date', \
                                        sas_to_date_type_udf(F.col('arrival_date')))
    df_fact_immi = df_fact_immi.withColumn('departure_date', \
                                        sas_to_date_type_udf(F.col('departure_date')))

    # write df_fact_immi table to parquet files partitioned by state and city
    df_fact_immi.write.mode("overwrite").partitionBy('state_code')\
                    .parquet(path=output_data + 'df_fact_immi')

    logging.info("Start processing df_dim_ident")
    # extract columns to create df_dim_ident table
    df_dim_ident = df_immi.select('cicid', 'i94cit', 'i94res',\
                                  'biryear', 'gender').distinct()\
                          .withColumn("ident_id", F.monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cicid', 'citizen_country', 'residence_country',\
                   'birth_year', 'gender']
    df_dim_ident = rename_columns(df_dim_ident, new_columns)

    # write df_dim_ident table to parquet files
    df_dim_ident.write.mode("overwrite")\
                     .parquet(path=output_data + 'df_dim_ident')

    logging.info("Start processing df_dim_flight")
    # extract columns to create df_dim_flight table
    df_dim_flight = df_immi.select('cicid', 'airline', 'admnum', 'fltno', 'visatype').distinct()\
                         .withColumn("flight_id", F.monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cicid', 'airline', 'admin_num', 'flight_num', 'visa_type']
    df_dim_flight = rename_columns(df_dim_flight, new_columns)
    df_dim_flight = df_dim_flight.filter('flight_num is not null null')
    # write df_dim_flight table to parquet files
    df_dim_flight.write.mode("overwrite")\
                    .parquet(path=output_data + 'df_dim_flight')



def process_label_descriptions(spark, input_data, output_data):
    """ Parsing label desctiption file to get codes of country, city, state, visa and transportation
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """

    logging.info("Start processing label descriptions")
    
    label_file = os.path.join(input_data + "I94_SAS_Labels_Descriptions.SAS")
    
    country_cols = ['code', 'country']
    df_country_code = sas_labels_descr_parser(label_file, 'i94cntyl', country_cols)
    df_country_code = df_country_code.drop_duplicates()
    df_country_code = spark.createDataFrame(df_country_code)
    df_country_code = df_country_code.withColumn("code", df_country_code["code"].cast(IntegerType()))
    
    df_country_code.write.mode("overwrite").parquet(path=output_data + 'df_country_code')

    city_cols = ['code', 'city']
    df_city_code = sas_labels_descr_parser(label_file, 'i94prtl', city_cols)
    df_city_code = df_city_code.drop_duplicates()
    
    spark.createDataFrame(df_city_code)\
                    .write.mode("overwrite")\
                        .parquet(path=output_data + 'df_city_code')

    state_cols = ['code', 'state']
    df_state_code = sas_labels_descr_parser(label_file, 'i94addrl', state_cols)
    df_state_code = df_state_code.drop_duplicates()
    
    spark.createDataFrame(df_state_code)\
                    .write.mode("overwrite")\
                        .parquet(path=output_data + 'df_state_code')
    
    visa_cols = ['code', 'type']
    df_visa_code = sas_labels_descr_parser(label_file, 'I94VISA', visa_cols)
    df_visa_code = df_visa_code.drop_duplicates()
    df_visa_code = spark.createDataFrame(df_visa_code)
    df_visa_code = df_visa_code.withColumn("code", df_visa_code["code"].cast(IntegerType()))

    df_visa_code.write.mode("overwrite").parquet(path=output_data + 'df_visa_code')
    
    trans_cols = ['code', 'mode']
    df_transportation = sas_labels_descr_parser(label_file, 'i94model', trans_cols)
    df_transportation = df_transportation.drop_duplicates()
    df_transportation = spark.createDataFrame(df_transportation)
    df_transportation = df_transportation.withColumn("code", df_transportation["code"].cast(IntegerType()))
    
    df_transportation.write.mode("overwrite").parquet(path=output_data + 'df_transportation')
    


def process_demography_data(spark, input_data, output_data):
    """ Process demograpy data to get df_dim_city_pop 
     and df_dim_city_stats table
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """

    logging.info("Start processing dim_demog_populaiton")
    # read demography data file
    demog_path = os.path.join(input_data + 'source_data/us-cities-demographics.csv')
    df_city_dem = spark.read.format('csv').options(header=True, delimiter=';', inferSchema=True).load(demog_path)


    df_dim_city_pop = df_city_dem.select(['City', 'State', 'Male Population', 'Female Population', \
                              'Number of Veterans', 'Foreign-born', 'Race']).distinct() \
                              .withColumn("city_pop_id", F.monotonically_increasing_id())


    new_columns = ['city', 'state', 'male_population', 'female_population', \
                   'num_vetarans', 'foreign_born', 'race']
    df_dim_city_pop = rename_columns(df_dim_city_pop, new_columns)
    
    df_dim_city_pop = df_dim_city_pop.withColumn('city', F.upper(F.col('city')))
    df_dim_city_pop = df_dim_city_pop.withColumn('state', F.upper(F.col('state')))

    # write df_dim_city_pop table to parquet files
    df_dim_city_pop.write.mode("overwrite")\
                        .parquet(path=output_data + 'df_dim_city_pop')

    
    logging.info("Start processing df_dim_city_stats")
    df_dim_city_stats = df_city_dem.select(['City', 'State', 'Median Age', 'Average Household Size'])\
                             .distinct()\
                             .withColumn("city_stats_id", F.monotonically_increasing_id())

    new_columns = ['city', 'state', 'median_age', 'avg_household_size']
    df_dim_city_stats = rename_columns(df_dim_city_stats, new_columns)
    
    df_dim_city_stats = df_dim_city_stats.withColumn('city', F.upper(F.col('city')))
    df_dim_city_stats = df_dim_city_stats.withColumn('state', F.upper(F.col('state')))


    # write dim_demog_statistics table to parquet files
    df_dim_city_stats.write.mode("overwrite")\
                        .parquet(path=output_data + 'df_dim_city_stats')




    
def main():
    spark = create_spark_session()
    input_data = SOURCE_S3_BUCKET
    output_data = DEST_S3_BUCKET
    
    process_immigration_data(spark, input_data, output_data)    
    process_label_descriptions(spark, input_data, output_data)
    process_demography_data(spark, input_data, output_data)
    logging.info("Data processing completed")


if __name__ == "__main__":
    main()