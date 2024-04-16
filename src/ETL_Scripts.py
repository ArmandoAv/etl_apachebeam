"""
    File with sql scripts
"""

# The library is imported
from ETL_Params import *


# Country source table count
country_source_table = f"""SELECT COUNT(*) AS INSERT_ROWS 
FROM   {tb_final_source_country}"""

# Country source table extract
country_source_extract = f"""SELECT *
FROM   {tb_final_source_country}"""
