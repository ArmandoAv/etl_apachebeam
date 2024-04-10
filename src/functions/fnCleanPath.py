"""
    Deletes the created files
"""

# The library is imported
import os


# Clean paths for the new ETL process
def fnCleanPath(output_path, temp_path, logs_path, file_countries):

    # Clean tmp path
    delete_temp_path = os.listdir(temp_path)

    if len(delete_temp_path) > 0:
        for delete in delete_temp_path:
            os.remove(temp_path + delete)

    # Clean output path
    delete_output_path = os.listdir(output_path)

    if len(delete_output_path) > 0:
        for delete in delete_output_path:
            os.remove(output_path + delete)

    # Clean log files
    delete_log_path = [file for file in os.listdir(logs_path) if file.endswith('.log')]
    if len(delete_log_path) > 0:
        for delete in delete_log_path:
            os.remove(logs_path + delete)
    
    # Clean country file
    os.remove(file_countries)
