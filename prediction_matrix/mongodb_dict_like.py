__author__ = 'jhajagos'


try:
    import pymongo
except ImportError:
    pass

class DictMappingMongo(object):
    """Create a dictionary like object that is backed by Mongodb Collection"""

    def __init__(self, mongo_client_obj, database_name, collection_name, name_to_use_as_key):
        self.mongo_client_obj = mongo_client_obj
        self.database_name = database_name
        self.collection_name = collection_name
        self.name_to_use_as_key = name_to_use_as_key

    def __getitem__(self, key):
        pass

    def __setitem__(self, key, val):
        pass

    def __len__(self):
        pass

