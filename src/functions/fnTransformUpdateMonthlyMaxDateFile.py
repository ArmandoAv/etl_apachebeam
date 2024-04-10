"""
    Create a file with updated columns
"""

# The libraries are imported
import apache_beam as beam
import os

# Update columns
def fnTransformUpdateMonthlyMaxDateFile(input_file1, input_file2, tmp_file, output_file, activity_header, file_sufix_tmp, file_sufix):

    # Define a function to split file
    def split_record(line):
        return line.split(',')

    # Define a function to create a tuple (key, value)
    def create_key_value(row, key_index):
        #key = row[key_index]
        key = tuple(row[i] for i in key_index)
        return (key, row)

    # Define a function to create a tuple all columns (key, value)
    def create_key_value_all_columns(row):
        key = tuple(row)
        return (key, row)

    # Define a function to convert record to string
    def convert_to_string(record):
        return ','.join(record)

    # Define a function to update columns with max date
    def update_values(row):
        key, values = row
        primary_data, secondary_data = values
        updated_primary_data = []
        if len(primary_data) > 0:
            updated_primary_data = secondary_data
        return updated_primary_data

    try:
        # Define the pipeline to update columns in parts
        with beam.Pipeline() as pipeline:
            principal_file = (
                pipeline
                # Create a tuple (key, value) with the principal file
                | 'ReadPrincipalFile' >> beam.io.ReadFromText(input_file1 + file_sufix, skip_header_lines = 1)
                | 'SplitPrincipalFile' >> beam.Map(lambda line: split_record(line))
                | 'CreateKeyValuePrincipal' >> beam.Map(lambda row: create_key_value(row, [0, 1, 2, 3]))
            )

            archivo_secundario_data = (
                pipeline
                # Create a tuple (key, value) with the secondary file
                | 'ReadSecondaryFile' >> beam.io.ReadFromText(input_file2, skip_header_lines = 1)
                | 'SplitSecondaryFile' >> beam.Map(lambda line: split_record(line))
                | 'CreateKeyValueSecondary' >> beam.Map(lambda row: create_key_value(row, [1, 2, 8, 3]))
            )

            joined_data = (
                (principal_file, archivo_secundario_data)
                # Update the columns and write the result into an output file
                | 'JoinData' >> beam.CoGroupByKey()
                | 'UpdateValues' >> beam.Map(update_values)
                | 'FlattenSublist' >> beam.FlatMap(lambda sublist: sublist)
                | 'ConvertColumnsToString' >> beam.Map(convert_to_string)
                | 'WriteToFile' >> beam.io.WriteToText(tmp_file, file_name_suffix = file_sufix, header = activity_header)
            )

        os.rename(tmp_file + file_sufix_tmp, output_file)

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nUpdate columns has failed. Please check the following error:\n", str(e))
        raise
