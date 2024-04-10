# Common Errors

In this file some common errors will be listed and how they can be fixed.

## Extract Process

### Error

py4j.protocol.Py4JJavaError: An error occurred while calling o52.csv.
: org.apache.spark.sql.AnalysisException: Path does not exist: file:/C:/Users/armando/Desktop/Git/etl_pyspark/input/id_driver.csv;

### Cause

The file id_driver.csv doesn't exist in the input path

### Posible solution

To correct it, you must run the create process again. If the problem persist, you need to copy the file id_driver_yyyymmdd.csv from the processed path to the input path and rename the file to id_driver.csv. If the file also doesn't exist in the processed path, you must truncate the DRIVER table and run the create process again.

### Error

ValueError: The file uber_data_yyyymmdd.csv has been crated in output path without records.
Please check because there are no records in the file, as it is created from the json input file.

### Cause

Sometimes the python job Extract.py can be created a file without records.

### Possible solution

To correct it you must run the python job Extract.py again

## Transform Process

### Error

py4j.protocol.Py4JJavaError: An error occurred while calling o90.csv.
: org.apache.spark.sql.AnalysisException: Path does not exist: file:/C:/...../etl_pyspark/output/uber_data_yyyymmdd.csv;

### Cause

This error is caused because the previous extract process has not been run. It may also be because the previous process was executed in previous days.

### Possible solution

To correct it you must to execute the extract process. If the process was already executed in previous days, the file must be renamed with the current date. Once any of the above options are done, run the transform process again.

### Error

ValueError: The file cat_locacion_recogida_yyyymmdd.csv has been crated in output path without records.
Please check because there aren't records in the file.

### Cause

Sometimes the python job Transform.py can be created a file without records.

### Possible solution

To correct it you must run the python job Transform.py again

## Load Process

### Error

py4j.protocol.Py4JJavaError: An error occurred while calling o56.save.
: java.lang.ClassNotFoundException: Failed to find data source: com.microsoft.sqlserver.jdbc.spark. Please find packages at http://spark.apache.org/third-party-projects.html

### Cause

This error is caused by the apache-spark-sql-connector.jar file isn't in the path C:\spark\jars

### Possible solution

To correct it you must to copy the apache-spark-sql-connector.jar file from the software path to the path C:\spark\jars

### Error

py4j.protocol.Py4JJavaError: An error occurred while calling o46.load.
: org.apache.spark.sql.AnalysisException: Path does not exist: file:/C:/...../etl_pyspark/output/cat_tarifa_yyyymmdd.csv;

### Cause

This error is caused because the previous extract and transform processes have not been run. It may also be because the previous processes were executed in previous days.

### Possible solution

To correct it you must to execute the extract and transform processes. If the processes were already executed in previous days, the files must be renamed with the current date. Once any of the above options are done, run the load process again.

### Error

py4j.protocol.Py4JJavaError: An error occurred while calling o56.save. : com.microsoft.sqlserver.jdbc.SQLServerException: No se pudo realizar la conexi¾n TCP/IP al host localhost, puerto 1433. Error: "Connection refused: connect. Verifique las propiedades de conexi¾n, compruebe que hay una instancia de SQL Server ejecutßndose en el host y aceptando las conexiones TCP/IP en el puerto y compruebe que no hay ning·n firewall bloqueando las conexiones TCP en el puerto.".

### Cause

This error is caused by the database server being down.

### Possible solution

To correct it you must open the SQL Server Management Studio and activate the server. If the errror continues, you shpuld check in the SQL Server Configuration Manager that TCP/IP connections are enabled and within TCP/IP in the IP Adresses tab in the TCP Port options you have the number of port 1433.

### Error

The file fact_final_pago_viaje_yyyymmdd.bad has been crated in bad path wit "n" records.
Please check because there are records in the bad file.

### Cause

This error is caused by the final files that had been created since when loading these files into their tables, an ID is generated to each record and when the final file of the table is created FACT_PAGO_VIAJE takes the IDs created as part of its columns.

### Possible solution

To correct it, you must run the python Load.py job again, so that the table IDs can be generated and the error is fixed.

```
spark-submit Load.py
```

### Error

ValueError: The TABLE_NAME table hasn't records loaded.
Please check because there aren't records in the table.

### Cause

This error is caused when a file that has to load a table doesn't have records and the table to load does not have records either. This cannot be possible, because each table would have to be loaded when it has no data loaded.

### Possible solution

To correct it, you must run the python Load.py job again, so that the table records can be generated and the error is fixed.

```
spark-submit Load.py
```

### Error

ValueError: The TABLE_NAME table doesn't have the same records loaded as the FINAL_FILE_yyyymmdd.csv file.
Please check because there doesn't the same records in the table.

### Cause

This error is caused when a final file doesn't have the same records as the records loaded in the table. This cannot be possible, because each table would have to be loaded when the same records as the final file.

### Possible solution

To correct it, you must run the python Load.py job again, so that the table records can be generated and the error is fixed.

```
spark-submit Load.py
```

### Warning

File "C:\spark\python\lib\py4j-0.10.7-src.zip\py4j\java_gateway.py", line 929, in \_get_connection
connection = self.deque.pop()
IndexError: pop from an empty deque

### Cause

This warning occurs when it try to delete a connection that does not exist the process corrects it when it have a connection again, however it sends this warning before continuing with the loads of the tables

## Contributing

If you find any new bugs and their possible fix, I hope you can help by contributing this will be **greatly appreciated**.
