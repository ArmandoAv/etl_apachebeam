"""
    Validates that the tables have data loaded
"""

# The libraries are imported
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyodbc
import csv
import os


# Validating the records loaded in the table
def fnCreateValidateTable(conn_str, query, path_count_table, path_count_ori, file_sufix):

    # Define a function to read data from SQL Server
    def read_from_sqlserver(query, conn_str):
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                yield from cursor

    # Define the pipeline
    try:
        with beam.Pipeline(options=PipelineOptions()) as p:
            (
                p
                | 'ReadFromSQLServer' >> beam.Create(read_from_sqlserver(query, conn_str))
                | 'ExtractResult' >> beam.Map(lambda x: x[0])
                | 'WriteResult' >> beam.io.WriteToText(path_count_ori, file_name_suffix = file_sufix)
            )

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nThe upload has failed. Please check the following error:\n", str(e))

    # Gets the number of records loaded in the table
    with open(path_count_table) as file:
        csv_reader = csv.reader(file, delimiter=',')

        for row in csv_reader:
            num_rec = row[0]

    # Deletes temp file
    if os.path.isfile(path_count_table):
        os.remove(path_count_table)
    
    # Returns the number of records
    return num_rec
