###################################################################################
#                                                                                 #
# Process: Load - ETL Apache Beam Process                                         #
# Created by: Armando Avila                                                       #
# Purpose: Clean the paths after ETL process                                      #
# Comment: Runs fnCleanPath.py file function with these parameters                #
#                    output_path                                                  #
#                    temp_path                                                    #
#                    logs_path                                                    #
#                    file_countries                                               #
#          Gets parameters from ETL_Params.py file                                #
# Created: 2024-03                                                                #
# Modified by:                                                                    #
# Modified date:                                                                  #
# Modifications:                                                                  #
#                                                                                 #
###################################################################################


# The libraries are imported
from ETL_Params import *
from datetime import datetime
from functions.fnCleanPath import *


# Formatted start date
datetime_start = datetime.today()
format_date_start = datetime_start.strftime("%d %B %Y %I:%M:%S %p")

print(f"\nThe Clean process has started at {format_date_start}.")


"""
    Paths are cleaned
"""
# Clean paths for the new ETL process
print("\nFinally the paths are cleaned after the ETL Process run...")
fnCleanPath(output_path=output_path,
            temp_path=temp_path,
            logs_path=logs_path,
            file_countries=path_countries_csv)


# The process has finished
datetime_finish = datetime.today()
format_date_finish = datetime_finish.strftime("%d %B %Y %I:%M:%S %p")

print(f"\nThe Clean process has finished at {format_date_finish}.\n")
