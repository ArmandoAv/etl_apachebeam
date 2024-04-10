"""
    Load table with the countries file
"""

# The libraries are imported
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyodbc


# Load table
def fnCreateLoadSourceTable(conn_str, table_name, path_country_csv):
    # Define a function to write data to SQL Server
    def write_to_sqlserver(rows, table_name, conn_str):
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                for row in rows:
                    insert_query = f'INSERT INTO {table_name} VALUES ({",".join(["?"]*len(row))})'
                    cursor.execute(insert_query, row)
                    conn.commit()
    
    try:
        # Define the pipeline
        with beam.Pipeline(options=PipelineOptions()) as p:
            (
                p
                | 'ReadFromCSV' >> beam.io.ReadFromText(path_country_csv, skip_header_lines=1)
                | 'ParseCSV' >> beam.Map(lambda line: tuple(line.split(',')))
                | 'WriteToSQLServer' >> beam.Map(lambda row: write_to_sqlserver([row], table_name, conn_str))
            )

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nThe upload has failed. Please check the following error:\n", str(e))
        raise
