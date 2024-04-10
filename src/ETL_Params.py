"""
    File with ETL parameters
"""

# The libraries are imported
from datetime import datetime
from dotenv import load_dotenv
from os import getenv


# Load the environment variables from the .env file
load_dotenv()

# Database connection string (source)
CONN_STR_SOURCE = getenv('CONN_STR_SOURCE')
DB_SOURCE_NAME = getenv('DB_SOURCE_NAME')
DB_SOURCE_SCHEMA = getenv('DB_SOURCE_SCHEMA')

# Database connection string (target)
CONN_STR_TARGET = getenv('CONN_STR_TARGET')
DB_TARGET_NAME = getenv('DB_TARGET_NAME')
COLLECT_TARGET_COVID_NAME = getenv('COLLECT_TARGET_COVID_NAME')
COLLECT_TARGET_DATE_NAME = getenv('COLLECT_TARGET_DATE_NAME')
COLLECT_TARGET_COUNTRY_NAME = getenv('COLLECT_TARGET_COUNTRY_NAME')

# Date parameter
current_datetime = datetime.today()
format_date = current_datetime.strftime("%Y%m%d")
format_log_date = current_datetime.strftime("%Y%m%d%H%M%S")

# Path parameters
input_path = "../input/"
output_path = "../output/"
temp_path = "../tmp/"
logs_path = "../logs/"

# Files names
file_source_activity_csv = "COVID-19-Activity.csv"
file_source_country_csv = "COVID-19-Countries.csv"
file_count_table = "Count-00000-of-00001.csv"
file_count_ori = "Count"
file_country_csv = "Countries"
file_activity_csv = "Activity"
file_activity_country = "Activity_Country"
file_activity_correct_col_csv = "Activity_Correct_col"
file_activity_wrong_col_csv = "Activity_Wrong_col"
file_activity_update_col_csv = "Activity_Update_col"
file_monthly_max_date = "Monthly_Max_Date"
file_monthly_update_max_date = "Monthly_Max_Update"

# Files sufix
csv_file_sufix_tmp = "-00000-of-00001.csv"
csv_file_sufix = ".csv"
json_file_sufix_tmp = "-00000-of-00001.json"
json_file_sufix = ".json"

# Path and files names
path_source_activity_csv = input_path + file_source_activity_csv
path_country_csv = input_path + file_source_country_csv
path_count_table = temp_path + file_count_table
path_count_ori = temp_path + file_count_ori
path_countries_tmp_csv = temp_path + file_country_csv
path_countries_csv = input_path + file_country_csv + "_" + format_date + csv_file_sufix
path_activity_tmp_csv = temp_path + file_activity_csv
path_activity_csv = output_path + file_activity_csv + "_" + format_date + csv_file_sufix
path_activity_country_csv = output_path + file_activity_country +  "_" + format_date + csv_file_sufix
path_activity_correct_col_csv = temp_path + file_activity_correct_col_csv
path_activity_wrong_col_csv = temp_path + file_activity_wrong_col_csv
path_activity_update_col_csv = temp_path + file_activity_update_col_csv
path_activity_json = temp_path + file_activity_country
path_activity_country_json = output_path + file_activity_country +  "_" + format_date + json_file_sufix
path_monthly_max_date_csv = temp_path + file_monthly_max_date
path_tmp_monthly_max_date_csv = temp_path + file_monthly_update_max_date
path_monthly_max_date_activity_csv = output_path + file_monthly_max_date +  "_" + format_date + csv_file_sufix
path_monthly_max_date_json = temp_path + file_monthly_max_date
path_monthly_max_date_activity_json = output_path + file_monthly_max_date +  "_" + format_date + json_file_sufix
path_countries_tmp_json = temp_path + file_country_csv
path_countries_json = output_path + file_country_csv +  "_" + format_date + json_file_sufix

# Files header
activity_header = 'PEOPLE_POSITIVE_CASES_COUNT,COUNTY_NAME,PROVINCE_STATE_NAME,REPORT_DATE,CONTINENT_NAME,DATA_SOURCE_NAME,PEOPLE_DEATH_NEW_COUNT,COUNTY_FIPS_NUMBER,COUNTRY_ALPHA_3_CODE,COUNTRY_SHORT_NAME,COUNTRY_ALPHA_2_CODE,PEOPLE_POSITIVE_NEW_CASES_COUNT,PEOPLE_DEATH_COUNT'
max_date_header = 'COUNTY_NAME,PROVINCE_STATE_NAME,COUNTRY_ALPHA_3_CODE,REPORT_DATE'
country_header = 'COUNTRY_ALPHA_3_CODE,COUNTRY_SHORT_NAME,COUNTRY_ALPHA_2_CODE'

# Logs files
create_log = "Create_Process_" + format_log_date + ".log"
extract_log = "Extract_Process_" + format_log_date + ".log"
transform_log = "Transform_Process_" + format_log_date + ".log"
load_log = "Load_Process_" + format_log_date + ".log"

# Paths and logs file names
path_create_log_name = logs_path + create_log
path_extract_log_name = logs_path + extract_log
path_transform_log_name = logs_path + transform_log
path_load_log_name = logs_path + load_log

# Table source name
tb_source_country = "COVID_19_COUNTRIES"

# Complete table source name
tb_final_source_country = DB_SOURCE_NAME + "." + DB_SOURCE_SCHEMA + "." + tb_source_country

# Batch size
batch_size_country = 250
batch_size_date = 2500
batch_size_covid = 10000
