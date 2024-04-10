###################################################################################
#                                                                                 #
# Process: Load - ETL Apache Beam Process                                         #
# Created by: Armando Avila                                                       #
# Purpose: Loads the tables with the generated files                              #
# Comment: Runs fnLoadDeleteCollection.py file function with these parameters     #
#                    mongodb_uri                                                  #
#                    database_name                                                #
#                    collection_name                                              #
#          Runs fnLoadWriteCollection.py file function with these parameters      #
#                    mongodb_uri                                                  #
#                    database_name                                                #
#                    collection_name                                              #
#                    input_file                                                   #
#                    batch_size                                                   #
#          Gets parameters from ETL_Params.py file                                #
# Created: 2024-03                                                                #
# Modified by:                                                                    #
# Modified date:                                                                  #
# Modifications:                                                                  #
#                                                                                 #
###################################################################################


# The libraries are imported
from ETL_Params import *
from functions.fnLoadWriteCollection import *
from functions.fnLoadDeleteCollection import *
from datetime import datetime
import logging


# Create log file
logging.basicConfig(filename=path_load_log_name,
                    format='%(asctime)s:%(levelname)s:%(message)s',
                    datefmt='%d/%m/%Y %I:%M:%S %p',
                    level=logging.DEBUG)
logging.debug('This message should appear on the console')
logging.info('So should this')
logging.warning('And this, too')
logging.error('It is a important message')

# Formatted start date
datetime_start = datetime.today()
format_date_start = datetime_start.strftime("%d %B %Y %I:%M:%S %p")

print(f"\nThe Load process has started at {format_date_start}.")
print("\nThe tables are being loading...\n")


"""
    Load json file countries in MongoDB collection country
"""
# Delete all documents from country collection
print(f"Delete all documents from the {COLLECT_TARGET_COUNTRY_NAME} collection if they exist...\n")
fnLoadDeleteCollection(mongodb_uri=CONN_STR_TARGET,
                       database_name=DB_TARGET_NAME,
                       collection_name=COLLECT_TARGET_COUNTRY_NAME)

# Load all documents to the country collection
print(f"Load the documents to the {COLLECT_TARGET_COUNTRY_NAME} collection...\n")
fnLoadWriteCollection(mongodb_uri=CONN_STR_TARGET,
                      database_name=DB_TARGET_NAME,
                      collection_name=COLLECT_TARGET_COUNTRY_NAME,
                      input_file=path_countries_json,
                      batch_size=batch_size_country)


"""
    Load json file dates in MongoDB collection date
"""
# Delete all documents from date collection
print(f"Delete all documents from the {COLLECT_TARGET_DATE_NAME} collection if they exist...\n")
fnLoadDeleteCollection(mongodb_uri=CONN_STR_TARGET,
                       database_name=DB_TARGET_NAME,
                       collection_name=COLLECT_TARGET_DATE_NAME)

# Load all documents to the date collection
print(f"Load the documents to the {COLLECT_TARGET_DATE_NAME} collection...\n")
fnLoadWriteCollection(mongodb_uri=CONN_STR_TARGET,
                     database_name=DB_TARGET_NAME,
                     collection_name=COLLECT_TARGET_DATE_NAME,
                     input_file=path_monthly_max_date_activity_json,
                     batch_size=batch_size_date)


"""
    Load json file covid in MongoDB collection covid
"""
# Delete all documents from date collection
print(f"Delete all documents from the {COLLECT_TARGET_COVID_NAME} collection if they exist...\n")
fnLoadDeleteCollection(mongodb_uri=CONN_STR_TARGET,
                       database_name=DB_TARGET_NAME,
                       collection_name=COLLECT_TARGET_COVID_NAME)

# Load all documents to the date collection
print(f"Load the documents to the {COLLECT_TARGET_COVID_NAME} collection...\n")
fnLoadWriteCollection(mongodb_uri=CONN_STR_TARGET,
                     database_name=DB_TARGET_NAME,
                     collection_name=COLLECT_TARGET_COVID_NAME,
                     input_file=path_activity_country_json,
                     batch_size=batch_size_covid)


# The process has finished
datetime_finish = datetime.today()
format_date_finish = datetime_finish.strftime("%d %B %Y %I:%M:%S %p")

print(f"\nThe Load process has finished at {format_date_finish}.\n")
