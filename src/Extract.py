###################################################################################
#                                                                                 #
# Process: Extract - ETL Apache Beam Process                                      #
# Created by: Armando Avila                                                       #
# Purpose: Gets an interface from SQL Server to a csv file                        #
# Comment: Runs fnExtractCountriesFile.py file function with these parameters     #
#                   conn_str                                                      #
#                   query                                                         #
#                   path_countries_tmp_csv                                        #
#                   path_countries_csv                                            #
#                   file_sufix_tmp                                                #
#                   file_sufix                                                    #
#                   header                                                        #
#          Runs fnDeleteFile.py file function with these parameters               #
#                   path                                                          #
#                   file_delete                                                   #
#          Gets parameters from ETL_Params.py file                                #
#          Gets file schemas from ETL_Schemas.py file                             #
# Created: 2024-03                                                                #
# Modified by:                                                                    #
# Modified date:                                                                  #
# Modifications:                                                                  #
#                                                                                 #
###################################################################################


# The libraries are imported
from ETL_Params import *
from ETL_Scripts import *
from functions.fnExtractCountriesFile import *
from functions.fnDeleteFile import *
from datetime import datetime
import logging


# Create log file
logging.basicConfig(filename=path_extract_log_name,
                    format='%(asctime)s:%(levelname)s:%(message)s',
                    datefmt='%d/%m/%Y %I:%M:%S %p',
                    level=logging.DEBUG)
logging.debug('This message should appear on the console')
logging.info('So should this')
logging.warning('And this, too')

# Formatted start date
datetime_start = datetime.today()
format_date_start = datetime_start.strftime("%d %B %Y %I:%M:%S %p")

print(f"\nThe Extract process has started at {format_date_start}.")

# Delete past files or directories in temp path
print(f"\nDelete possible tmp files...\n")
fnDeleteFile(path=temp_path,
             file_delete=path_countries_csv)

# Create csv Countries file in the output path
print(f"Created the csv Countries file...\n")
fnExtractCountriesFile(conn_str=CONN_STR_SOURCE,
                       query=country_source_extract,
                       path_countries_tmp_csv=path_countries_tmp_csv,
                       path_countries_csv=path_countries_csv,
                       file_sufix_tmp=csv_file_sufix_tmp,
                       file_sufix=csv_file_sufix,
                       header=country_header)

# Formatted end date
datetime_finish = datetime.today()
format_date_finish = datetime_finish.strftime("%d %B %Y %I:%M:%S %p")

# The process has finished
print("The Extract process has finished successfully.")
print(f"\nThe Extract process has finished at {format_date_finish}.\n")
