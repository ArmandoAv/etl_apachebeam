"""
    Concatenate files
"""

# The libraries are imported
import apache_beam as beam
import os


# Concatenate files
def fnTransformConcatFile(input_file1, input_file2, tmp_file, output_file, file_sufix_tmp, file_sufix, header):

    try:
        # Define the pipeline
        with beam.Pipeline() as pipeline:
            # Input files
            file1 = pipeline | 'ReadFromCSV1' >> beam.io.ReadFromText(input_file1 + file_sufix_tmp, skip_header_lines = 1)
            file2 = pipeline | 'ReadFromCSV2' >> beam.io.ReadFromText(input_file2 + file_sufix_tmp, skip_header_lines = 1)

            # Concatenate the collections
            concat_files = (file1, file2) | 'ConcatenateFiles' >> beam.Flatten()

            # Save the combined collection to a new csv file
            concat_files | 'WriteCollectionToFile' >> beam.io.WriteToText(tmp_file, file_name_suffix = file_sufix, header = header)

        os.rename(tmp_file + file_sufix_tmp, output_file)

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nThe concatenation has failed. Please check the following error:\n", str(e))
        raise
