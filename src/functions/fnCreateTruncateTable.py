"""
    Truncate table
"""

# The libraries are importedTengo el siguiente codigo:
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyodbc


# Truncate table
def fnCreateTruncateTable(conn_str, table_name):
    # Define a function to write data to SQL Server
    def truncate_table(rows, table_name, conn_str):
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f'TRUNCATE TABLE {table_name}')
                conn.commit()
    
    try:
        # Define the pipeline
        with beam.Pipeline(options=PipelineOptions()) as p:
            # Execute the truncate_table function directly
            truncate_table(None, table_name, conn_str)

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nThe truncate has failed. Please check the following error:\n", str(e))
        raise
