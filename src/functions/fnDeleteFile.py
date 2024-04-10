"""
    Delete files or directories
"""

# The libraries are imported
import shutil
import os


# Delete files or directories in temp path
def fnDeleteFile(path, file_delete):

    # Gets the content of the temp path
    dir_contents = os.listdir(path)
    
    # If the path has files or directories they are deleted
    if len(dir_contents) > 0:
        print("There are some files or directories to delete.\n")

        for delete in dir_contents:
            file_path = os.path.join(path, delete)
            
            # Deletes possible files
            if os.path.isfile(file_path):
                os.remove(file_path)

            # Deletes possible directories
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)

    # If path_file_delete parameter is not null, delete the file
    if file_delete is not None:

        # Gets the file to delete
        if os.path.isfile(file_delete):
            print("There is a file to delete.\n")

            os.remove(file_delete)
