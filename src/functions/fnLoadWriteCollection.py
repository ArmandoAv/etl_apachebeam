"""
    Write the documents to a MongoDB collection
"""

# The libraries are imported
import apache_beam as beam
from apache_beam.io.mongodbio import WriteToMongoDB
import json

# Write the documents
def fnLoadWriteCollection(mongodb_uri, database_name, collection_name, input_file, batch_size):

    try:
        # Define the pipeline to write to the collection
        with beam.Pipeline() as pipeline:
            # Read the json file
            read_data = (
                pipeline
                | 'ReadJSON' >> beam.io.ReadFromText(input_file)
                | 'ParseJSON' >> beam.Map(lambda x: json.loads(x))
            )

            # Write the documents 
            write_to_mongodb = (
                read_data
                | 'WriteToMongoDB' >> WriteToMongoDB(uri = mongodb_uri, db = database_name, coll = collection_name, batch_size = batch_size)
            )

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nWriting to MongoDB collection has failed. Please check the following error:\n", str(e))
        raise
