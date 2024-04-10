"""
    Delete all documents from a collection
"""

# The library is imported
import pymongo

# Delete collections
def fnLoadDeleteCollection(mongodb_uri, database_name, collection_name):

    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(mongodb_uri)
        db = client[database_name]
        collection = db[collection_name]

        # Delete all documents
        collection.delete_many({})

        # Close the connection to MongoDB
        client.close()

    except Exception as e:
        # An exception is raised with a message containing the error
        print("\nDelete the documents from a collection has failed. Please check the following error:\n", str(e))
        raise
