"""
    Create the Countries csv file
"""

# The libraries are importedTengo el siguiente codigo: 
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyodbc
import os


# Extract the data from the source database and save it into a csv file
def fnExtractCountriesFile(conn_str, query, path_countries_tmp_csv, path_countries_csv, file_sufix_tmp, file_sufix, header):

    # Define a function to write data to SQL Server
    def read_from_sqlserver(query, conn_str):
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                yield from cursor

    # Convert record to string
    def convert_to_string(record):
        return ','.join(record)

    try:
        # Define the pipeline
        with beam.Pipeline(options=PipelineOptions()) as p:
            (
                p
                | 'ReadFromSQLServer' >> beam.Create(read_from_sqlserver(query, conn_str))
                | 'Convert correct to string' >> beam.Map(convert_to_string)
                | 'WriteResulta' >> beam.io.WriteToText(path_countries_tmp_csv, file_name_suffix = file_sufix, header = header)
            )

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nThe upload has failed. Please check the next error:\n", str(e))
        raise

    # Delete possible imput file
    if os.path.isfile(path_countries_csv):
        os.remove(path_countries_csv)

    # Rename temporary file to input file ame
    os.rename(path_countries_tmp_csv + file_sufix_tmp, path_countries_csv)
