"""
    Create two files with correct countries code and wrong countries code
"""

# The libraries are imported
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# Clean activity file
def fnTransformCountryActivityFile(input_file, output_file1, output_file2, file_sufix, header):

    # Define a function to split file
    def split_record(line):
        return line.split(',')

    # Define a function to convert record to string
    def convert_to_string(record):
        return ','.join(record)

    # Define a function to validate data in column
    def correct_data_column(row):
        return row[8] != ''

    # Define a function to validate no data in column
    def wrong_no_data_column(row):
        return row[8] == ''

    try:
        # Define the first pipeline with correct columns
        with beam.Pipeline(options=PipelineOptions()) as p:
            (p
            | 'ReadFromCSV' >> beam.io.ReadFromText(input_file, skip_header_lines = 1)
            
            # Write the output to new CSV file with the correct column with data
            | 'SplitRecords' >> beam.Map(split_record)
            | 'FilterRows' >> beam.Filter(correct_data_column)
            | 'ConvertCorrectColumnsToString' >> beam.Map(convert_to_string)
            | 'WriteCorrectColumnsToFile' >> beam.io.WriteToText(output_file1, file_name_suffix = file_sufix, header = header)
            )

        # Define the second pipeline with wrong columns
        with beam.Pipeline(options=PipelineOptions()) as p:
            (p
            | 'ReadFromCSV' >> beam.io.ReadFromText(input_file, skip_header_lines = 1)
            
            # Write the output to new CSV file with the wrong column with data
            | 'SplitRecords' >> beam.Map(split_record)
            | 'FilterRows' >> beam.Filter(wrong_no_data_column)
            | 'ConvertWrongColumnsToString' >> beam.Map(convert_to_string)
            | 'WriteWrongColumnsToFile' >> beam.io.WriteToText(output_file2, file_name_suffix = file_sufix, header = header)
            )

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nActivity file cleanup failed. Please check the following error:\n", str(e))
        raise
