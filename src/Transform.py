#############################################################################################
#                                                                                           #
# Process: Transform - ETL Apache Beam Process                                              #
# Created by: Armando Avila                                                                 #
# Purpose: The activity file data is cleaned and the files that will be loaded into         #
#          the tables are created.                                                          #
# Comment: Run fnDeleteFile.py file function with these parameters                          #
#                    path                                                                   #
#                    file_delete                                                            #
#          Run fnTransformCleanActivityFile.py file function with these parameters          #
#                    input_file                                                             #
#                    output_file1                                                           #
#                    output_file2                                                           #
#                    file_sufix                                                             #
#                    header                                                                 #
#          Run fnTransformUpdateWrongFile.py file function with these parameters            #
#                    input_file1                                                            #
#                    input_file2                                                            #
#                    output_file                                                            #
#                    activity_header                                                        #
#                    file_sufix                                                             #
#                    file_sufix_tmp                                                         #
#          Run fnTransformCountryActivityFile.py file function with these parameters        #
#                    input_file                                                             #
#                    output_file1                                                           #
#                    output_file2                                                           #
#                    file_sufix                                                             #
#                    header                                                                 #
#          Run fnTransformConcatFile.py file function with these parameters                 #
#                    input_file1                                                            #
#                    input_file2                                                            #
#                    tmp_file                                                               #
#                    output_file                                                            #
#                    file_sufix_tmp                                                         #
#                    file_sufix                                                             #
#                    header                                                                 #
#          Runs fnTransformCreateJsonFile.py file function with these parameters            #
#                    input_file                                                             #
#                    tmp_file                                                               #
#                    output_file                                                            #
#                    file_schema                                                            #
#                    file_sufix_tmp                                                         #
#                    file_sufix                                                             #
#          Runs fnTransformMonthlyMaxDateFile.py file function with these parameters        #
#                    input_file                                                             #
#                    output_file                                                            #
#                    max_date_header                                                        #
#                    file_sufix_tmp                                                         #
#                    file_sufix                                                             #
#          Runs fnTransformUpdateMonthlyMaxDateFile.py file function with these parameters  #
#                    input_file1                                                            #
#                    input_file2                                                            #
#                    tmp_file                                                               #
#                    output_file                                                            #
#                    activity_header                                                        #
#                    file_sufix_tmp                                                         #
#                    file_sufix                                                             #
#          Get parameters from ETL_Params.py file                                           #
#          Gets file schemas from ETL_Schemas.py file                                       #
# Created: 2024-03                                                                          #
# Modified by:                                                                              #
# Modified date:                                                                            #
# Modifications:                                                                            #
#                                                                                           #
#############################################################################################


# The libraries are imported
from ETL_Params import *
from ETL_Schemas import *
from functions.fnDeleteFile import *
from functions.fnTransformConcatFile import *
from functions.fnTransformCreateJsonFile import *
from functions.fnTransformUpdateWrongFile import *
from functions.fnTransformCleanActivityFile import *
from functions.fnTransformMonthlyMaxDateFile import *
from functions.fnTransformCountryActivityFile import *
from functions.fnTransformUpdateMonthlyMaxDateFile import *
from datetime import datetime
import logging


# Create log file
logging.basicConfig(filename=path_transform_log_name,
                    format='%(asctime)s:%(levelname)s:%(message)s',
                    datefmt='%d/%m/%Y %I:%M:%S %p',
                    level=logging.DEBUG)
logging.debug('This message should appear on the console')
logging.info('So should this')
logging.warning('And this, too')

# Formatted start date
datetime_start = datetime.today()
format_date_start = datetime_start.strftime("%d %B %Y %I:%M:%S %p")

print(f"\nThe Transform process has started at {format_date_start}.")
print("\nThe transformations are being processed...\n")


"""
    Clean the Activity csv file, this files has
    empty columns, doble quotes, commas in some
    descriptions
"""

# Delete old files or directories in temp path
print("Delete possible old files...\n")
fnDeleteFile(path=temp_path,
             file_delete=path_activity_csv)

# Clean the Activity file and create two files with correct and wrong columns
print("Slipt Activity file between a file with correct columns and a\n"\
      "file with columns with doble quotes, commas in some descriptions.\n"\
      "Finally, fixed the second file.\n")
fnTransformCleanActivityFile(input_file=path_source_activity_csv,
                             output_file1=path_activity_correct_col_csv,
                             output_file2=path_activity_wrong_col_csv,
                             file_sufix=csv_file_sufix,
                             header=activity_header)

# Concatenate the correct and wrong columns files into a csv file in the output path
print("Concatenate the correct columns file and the fixed wrong columns file.\n")
fnTransformConcatFile(input_file1=path_activity_correct_col_csv,
                      input_file2=path_activity_wrong_col_csv,
                      tmp_file=path_activity_tmp_csv,
                      output_file=path_activity_csv,
                      file_sufix_tmp=csv_file_sufix_tmp,
                      file_sufix=csv_file_sufix,
                      header=activity_header)

# Delete old files or directories in temp path
print("Delete possible old files...\n")
fnDeleteFile(path=temp_path,
             file_delete=path_activity_country_csv)

# Validate the clean csv file and create two files with correct and wrong columns
print("Slipt the new Activity file between a file with correct columns\n"\
      "and a file with empty columns in some alpha code countries.\n")
fnTransformCountryActivityFile(input_file=path_activity_csv,
                               output_file1=path_activity_correct_col_csv,
                               output_file2=path_activity_wrong_col_csv,
                               file_sufix=csv_file_sufix,
                               header=activity_header)

# Updated the wrong columns files
print("Updated the file with empty columns in some alpha code countries\n"\
      "with the correct alpha codes.\n")
fnTransformUpdateWrongFile(input_file1=path_activity_wrong_col_csv,
                           input_file2=path_countries_csv,
                           output_file=path_activity_update_col_csv,
                           activity_header=activity_header,
                           file_sufix=csv_file_sufix,
                           file_sufix_tmp=csv_file_sufix_tmp)

# Concatenate the correct and wrong columns files into a csv file in the output path
print("Concatenate the correct columns file and the updated wrong columns file.\n")
fnTransformConcatFile(input_file1=path_activity_correct_col_csv,
                      input_file2=path_activity_update_col_csv,
                      tmp_file=path_activity_tmp_csv,
                      output_file=path_activity_country_csv,
                      file_sufix_tmp=csv_file_sufix_tmp,
                      file_sufix=csv_file_sufix,
                      header=activity_header)

# Delete old files or directories in temp path
print("Delete possible old files...\n")
fnDeleteFile(path=temp_path,
             file_delete=None)


"""
    Create the Activity json file for loading into the database
"""

# Create the Activity json file
print("Creating the Activity json file to upload to database...\n")
fnTransformCreateJsonFile(input_file=path_activity_country_csv,
                          tmp_file=path_activity_json,
                          output_file=path_activity_country_json,
                          file_schema=activity_json_schema,
                          file_sufix_tmp=json_file_sufix_tmp,
                          file_sufix=json_file_sufix)

# Delete old files or directories in temp path
print("Delete possible old files...\n")
fnDeleteFile(path=temp_path,
             file_delete=None)


"""
    Create the Monthly Max Date Activity json file for loading into the database
"""

# Create the monthly max date file
print("Creating the Monthly Max Date file...\n")
fnTransformMonthlyMaxDateFile(input_file=path_activity_country_csv,
                              output_file=path_monthly_max_date_csv, 
                              max_date_header=max_date_header,
                              file_sufix_tmp=csv_file_sufix_tmp, 
                              file_sufix=csv_file_sufix)

# Create the monthly max date file with all columns
print("Creating the Monthly Max Date file with all columns...\n")
fnTransformUpdateMonthlyMaxDateFile(input_file1=path_monthly_max_date_csv,
                                    input_file2=path_activity_country_csv,
                                    tmp_file=path_tmp_monthly_max_date_csv,
                                    output_file=path_monthly_max_date_activity_csv,
                                    activity_header=activity_header,
                                    file_sufix_tmp=csv_file_sufix_tmp,
                                    file_sufix=csv_file_sufix)

# Create the Activity json file
print("Creating the Activity json file to upload to database...\n")
fnTransformCreateJsonFile(input_file=path_monthly_max_date_activity_csv,
                          tmp_file=path_monthly_max_date_json,
                          output_file=path_monthly_max_date_activity_json,
                          file_schema=activity_json_schema,
                          file_sufix_tmp=json_file_sufix_tmp,
                          file_sufix=json_file_sufix)

# Delete old files or directories in temp path
print("Delete possible old files...\n")
fnDeleteFile(path=temp_path,
             file_delete=None)


"""
    Create the Country json file for loading into the database
"""

# Create the monthly max date file
print("Creating the Country json file to upload to database...\n")
fnTransformCreateJsonFile(input_file=path_countries_csv,
                          tmp_file=path_countries_tmp_json,
                          output_file=path_countries_json,
                          file_schema=country_json_schema,
                          file_sufix_tmp=json_file_sufix_tmp,
                          file_sufix=json_file_sufix)

# Delete old files or directories in temp path
print("Delete possible old files...\n")
fnDeleteFile(path=temp_path,
             file_delete=None)

# Formatted end date
datetime_finish = datetime.today()
format_date_finish = datetime_finish.strftime("%d %B %Y %I:%M:%S %p")

# The process has finished
print("The Transform process has finished successfully.")
print(f"\nThe Transform process has finished at {format_date_finish}.\n")
