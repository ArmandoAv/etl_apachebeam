"""
    Create a file with updated columns
"""

# The library is imported
import apache_beam as beam

# Update columns
def fnTransformUpdateWrongFile(input_file1, input_file2, output_file, activity_header, file_sufix, file_sufix_tmp):

    # Define a function to split file
    def split_record(line):
        return line.split(',')

    # Define a function to create a tuple (key, value)
    def create_key_value(row, key_index):
        key = row[key_index]
        return (key, row)

    # Define a function to convert record to string
    def convert_to_string(record):
        return ','.join(record)

    # Define a function to update columns
    def update_values(row):
        key, values = row
        primary_data, secondary_data = values
        updated_primary_data = [primary_data[:8] + secondary_data[0] + primary_data[11:] for primary_data in primary_data]
        return updated_primary_data

    try:
        # Define the pipeline to update columns in parts
        with beam.Pipeline() as pipeline:
            principal_file = (
                pipeline
                # Create a tuple (key, value) with the principal file
                | 'ReadPrincipalFile' >> beam.io.ReadFromText(input_file1 + file_sufix_tmp, skip_header_lines = 1)
                | 'SplitPrincipalFile' >> beam.Map(lambda line: split_record(line))
                | 'CreateKeyValuePrincipal' >> beam.Map(lambda row: create_key_value(row, 9))
            )

            secondary_file = (
                pipeline
                # Create a tuple (key, value) with the secondary file
                | 'ReadSecondaryFile' >> beam.io.ReadFromText(input_file2, skip_header_lines = 1)
                | 'SplitSecondaryFile' >> beam.Map(lambda line: split_record(line))
                | 'CreateKeyValueSecondary' >> beam.Map(lambda row: create_key_value(row, 1))
            )

            joined_data = (
                (principal_file, secondary_file)
                # Update the columns and write the result into an output file
                | 'JoinData' >> beam.CoGroupByKey()
                | 'UpdateValues' >> beam.Map(update_values)
                | 'FlattenSublist' >> beam.FlatMap(lambda sublist: sublist)
                | 'ConvertColumnsToString' >> beam.Map(convert_to_string)
                | 'WriteToFile' >> beam.io.WriteToText(output_file, file_name_suffix = file_sufix, header = activity_header)
            )

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nUpdate columns has failed. Please check the following error:\n", str(e))
        raise
