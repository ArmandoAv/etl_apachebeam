"""
    Create a monthly max date file
"""

# The libraries are imported
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import os

# Create max monthly date file
def fnTransformMonthlyMaxDateFile(input_file, output_file, max_date_header, file_sufix_tmp, file_sufix):

    # Define a function to split file
    def split_record(line):
        fields = line.split(',')
        date_str = fields[3]
        date = datetime.strptime(date_str, '%Y-%m-%d').date()
        year = date.year
        month = date.month

        return [fields[1], fields[2], fields[8], str(year), str(month), (date)]
        
    # Define a function to convert record (key, value) to string
    def convert_to_string(node):

        # Extract the sublist from the nodo
        sublist = node[0][0], node[0][1], node[0][2]
        date = node[1]

        # Convert all elements of the sublist to strings
        sublist = [str(elem) for elem in sublist]

        # Join all elements of the sublist and date with a comma
        elements = sublist[0], sublist[1], sublist[2], str(date)
        return ','.join(elements)

    # Define a function to extract the nodo (key, value)
    def extract_key_value(record):
        return (record[0:5]), record[5]

    try:
        # Define the pipeline
        with beam.Pipeline(options=PipelineOptions()) as p:
            (
            p
            # Read the CSV file
            | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines = 1)
            
            # Write the CSV file
            | 'Parse header' >> beam.Map(lambda line: split_record(line))
            # Extract the key and the value
            | 'Extract key value' >> beam.Map(lambda record: extract_key_value(record))
            # Group by key and dates in a node (key, value)
            | 'Group by key' >> beam.GroupByKey()
            # Get the max date
            | 'Max value' >> beam.Map(lambda x: (x[0], max(x[1]).strftime('%Y-%m-%d')))
            | 'Convert to String' >> beam.Map(lambda row: convert_to_string(row))
            | 'Write temp Output' >> beam.io.WriteToText(output_file, file_name_suffix = file_sufix, header = max_date_header)
            )

        os.rename(output_file + file_sufix_tmp, output_file + file_sufix)

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nThe creation of the monthly max date file has failed. Please check the following error:\n", str(e))
        raise
