###################################################################################
#                                                                                 #
# Process: Create - ETL Apache Beam Process                                       #
# Created by: Armando Avila                                                       #
# Purpose: Loads information into the table in the source database, as a batch    #
#          process system before to the ETL process begins                        #
# Comment: Runs fnCreateLoadSourceTable.py file function with these parameters    #
#                    conn_str                                                     #
#                    table                                                        #
#                    path_country_csv                                             #
#          Runs fnCreateValidateTable.py file function with these parameters      #
#                    conn_str                                                     #
#                    query                                                        #
#                    path_count_table                                             #
#                    path_count_ori                                               #
#                    file_sufix                                                   #
#          Runs fnCreateTruncateTable.py file function with these parameters      #
#                    conn_str                                                     #
#                    table                                                        #
#          Runs fnDeleteFile.py file function with these parameters               #
#                    path                                                         #
#                    file_delete                                                  #
#          Gets parameters from ETL_Params.py file                                #
#          Gets defined queries from ETL_Scripts.py file                          #
# Created: 2024-03                                                                #
# Modified by:                                                                    #
# Modified date:                                                                  #
# Modifications:                                                                  #
#                                                                                 #
###################################################################################


# The libraries are imported
from ETL_Params import *
from ETL_Scripts import *
from functions.fnCreateLoadSourceTable import *
from functions.fnCreateValidateTable import *
from functions.fnCreateTruncateTable import *
from functions.fnDeleteFile import *
import logging


# Create log file
logging.basicConfig(filename=path_create_log_name,
                    format='%(asctime)s:%(levelname)s:%(message)s',
                    datefmt='%d/%m/%Y %I:%M:%S %p',
                    level=logging.DEBUG)
logging.debug('This message should appear on the console')
logging.info('So should this')
logging.warning('And this, too')

# Formatted start date
datetime_start = datetime.today()
format_date_start = datetime_start.strftime("%d %B %Y %I:%M:%S %p")

print(f"\nThe Create process has started at {format_date_start}.\n")

# Truncate table into SQL Server
print("Truncate table...\n")
fnCreateTruncateTable(conn_str=CONN_STR_SOURCE,
                      table_name = tb_final_source_country)

# Delete past files or directories in temp path
print(f"Delete possible tmp files...\n")
fnDeleteFile(path=temp_path,
             file_delete=None)

# Load the countries data file into SQL Server
print("Starting upload to table...\n")
fnCreateLoadSourceTable(conn_str = CONN_STR_SOURCE,
                        table_name = tb_final_source_country,
                        path_country_csv=path_country_csv)

# Validating if the countries table was loaded
num_rec = fnCreateValidateTable(conn_str=CONN_STR_SOURCE,
                                query=country_source_table,
                                path_count_table=path_count_table,
                                path_count_ori=path_count_ori,
                                file_sufix=csv_file_sufix)
print(f"The table {tb_source_country} data has been loaded with {num_rec} records into SQL Server.\n")

# Formatted end date
datetime_finish = datetime.today()
format_date_finish = datetime_finish.strftime("%d %B %Y %I:%M:%S %p")

# The process has finished
print("The Create process has finished successfully.")
print(f"\nThe Create process has finished at {format_date_finish}.\n")
