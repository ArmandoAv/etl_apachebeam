"""
    Create two files with correct countries name and wrong countries name
"""

# The libraries are imported
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# Clean activity file
def fnTransformCleanActivityFile(input_file, output_file1, output_file2, file_sufix, header):

    # Define a function to split file
    def split_record(line):
        return line.split(',')

    # Define a function to replace comma with an empty string in column 9
    def remove_comma(record):
        new_record = ''.join(record[9:11])
        record = record[:9] + [new_record] + record[11:]
        return record

    # Define a function to delete double quotes
    def remove_quotes(record):
        return [field.replace('"', '') for field in record]

    # Define a function to delete asterisk
    def remove_asterisk(record):
        return [field.replace('*', '') for field in record]

    # Define a function to convert record to string
    def convert_to_string(record):
        return ','.join(record)

    # Define a function to correct number of columns
    def filter_correct_columns(record):
        return len(record) == 13

    # Define a function to wrong number of columns
    def filter_wrong_columns(record):
        return len(record) > 13

    try:
        # Define the first pipeline with correct columns
        with beam.Pipeline(options=PipelineOptions()) as p:
            (p
            | 'ReadFromCSV' >> beam.io.ReadFromText(input_file, skip_header_lines = 1)
            
            # Remove quotes an asterisks from each record
            | 'SplitRecords' >> beam.Map(split_record)
            | 'RemoveQuotes' >> beam.Map(remove_quotes)
            | 'RemoveAsterisk' >> beam.Map(remove_asterisk)
            
            # Write the output to new CSV file with the correct column count
            | 'FilterCorrectColumns' >> beam.Filter(filter_correct_columns)
            | 'ConvertCorrectColumnsToString' >> beam.Map(convert_to_string)
            | 'WriteCorrectColumnsToFile' >> beam.io.WriteToText(output_file1, file_name_suffix = file_sufix, header = header)
            )

        # Define the second pipeline with wrong columns
        with beam.Pipeline(options=PipelineOptions()) as p:
            (p
            | 'ReadFromCSV' >> beam.io.ReadFromText(input_file, skip_header_lines = 1)
            
            # Remove quotes an asterisks from each record
            | 'SplitRecords' >> beam.Map(split_record)
            | 'RemoveQuotes' >> beam.Map(remove_quotes)
            | 'RemoveAsterisk' >> beam.Map(remove_asterisk)
            
            # Write the output to new CSV file with the wrong column count
            | 'FilterWrongColumns' >> beam.Filter(filter_wrong_columns)
            | 'RemoveComma' >> beam.Map(remove_comma)
            | 'ConvertWrongColumnsToString' >> beam.Map(convert_to_string)
            | 'WriteWrongColumnsToFile' >> beam.io.WriteToText(output_file2, file_name_suffix = file_sufix, header = header)
            )

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nActivity file cleanup failed. Please check the following error:\n", str(e))
        raise
