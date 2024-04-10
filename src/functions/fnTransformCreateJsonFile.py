"""
    Create the Activity json file
"""

# The libraries are imported
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import os

# Create json file
def fnTransformCreateJsonFile(input_file, tmp_file, output_file, file_schema, file_sufix_tmp, file_sufix):

    # Define a function to split file
    def split_record(line):
        return line.split(',')

    # Define a function to create rows in json format 
    def serialize(schema, row):
        serialized_row = {}
        num_fields = len(file_schema["fields"])

        # Validate if the schema is a catalog
        if num_fields == 3:
            for i, field in enumerate(schema['fields']):
                serialized_row[field['name']] = row[i]
            return json.dumps(serialized_row)
        else:
            for i, field in enumerate(schema['fields']):
                if i == 0 or i == 6 or i == 11 or i == 12:
                    # Only fields with data will be used to create the json file
                    if row[i] != '':
                        serialized_row[field['name']] = int(row[i])
                else:
                    # Only fields with data will be used to create the json file
                    if row[i] != '':
                        serialized_row[field['name']] = row[i]
            return json.dumps(serialized_row)

    try:
        # Create the pipeline
        with beam.Pipeline(options=PipelineOptions()) as p:
            (p
            | 'Read header' >> beam.io.ReadFromText(input_file, skip_header_lines = 1)

            # Write the output to new JSON file
            | 'Parse header' >> beam.Map(lambda line: split_record(line))
            | 'Apply schema' >> beam.Map(lambda row: serialize(file_schema, row))
            | 'Write JSON' >> beam.io.WriteToText(tmp_file, file_name_suffix = file_sufix)
            )

        # Rename temporary file to input file ame
        os.rename(tmp_file + file_sufix_tmp, output_file)

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nThe creation of the Activity json file has failed. Please check the following error:\n", str(e))
        raise
