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


# Validation CAT_TARIFA
table_tarifa = f"""SELECT 
       IM.ID_TARIFA,
	   IM.TARIFA,
	   CAST(CURRENT_TIMESTAMP AS DATE) AS FCH_CARGA
FROM   {tb_final_source_country} AS IM
LEFT JOIN {tb_final_source_country} AS FN
ON  IM.ID_TARIFA = FN.ID_TARIFA
WHERE FN.ID_TARIFA IS NULL"""

num_rec_tb_tarifa = f"""SELECT 
       COUNT(*) AS NUM_REG
FROM   {tb_final_source_country} AS IM
LEFT JOIN {tb_final_source_country} AS FN
ON  IM.ID_TARIFA = FN.ID_TARIFA
WHERE FN.ID_TARIFA IS NULL"""


# Country source table count
country_source_table = f"""SELECT COUNT(*) AS INSERT_ROWS 
FROM   {tb_final_source_country}"""



# Final validation
log_final_carga = f"""SELECT 'FECHA_CARGA_TABLA' AS FCH_LOG,
       'HORA_CARGA_TABLA' AS HR_LOG,
       'NOMBRE_ARCHIVO' AS NOM_ARCHIVO,
	   'NUMERO_REGISTROS_DEL_ARCHIVO' AS REGISTROS_NOM_ARCHIVO,
	   'NOMBRE_TABLA_A_CARGAR' AS NOM_TABLA,
	   'REGISTROS_INSERTADOS_EN_TABLA' AS REGISTROS_INSERTADOS_TABLA,
	   'COMENTARIO_DE_CARGA_EN_TABLA' AS COMENTARIO
UNION ALL
SELECT  CAST(CAST(FCH_LOG AS DATE) AS VARCHAR) AS FCH_LOG,
        CAST(CAST(FCH_LOG AS TIME(0)) AS VARCHAR) AS HR_LOG,
        NOM_ARCHIVO,
	    CAST(REGISTROS_NOM_ARCHIVO AS VARCHAR) AS REGISTROS_NOM_ARCHIVO,
	    NOM_TABLA,
	    CAST(REGISTROS_INSERTADOS_TABLA AS VARCHAR) AS REGISTROS_INSERTADOS_TABLA,
	    COMENTARIO
FROM    {tb_final_source_country}
WHERE   FCH_LOG >= (SELECT MAX(FCH_LOG) AS FCH_LOG
                    FROM   {tb_final_source_country}
				    WHERE  ID_PROCESO = 1)"""
