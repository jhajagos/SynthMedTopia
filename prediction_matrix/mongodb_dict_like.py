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

        self.db = self.mongo_client_obj[self.database_name]
        self.collection = self.db[self.collection_name]

    def __getitem__(self, key):
        item_dict = self.collection.find_one({self.name_to_use_as_key: key})
        if item_dict is not None:
            return item_dict
        else:
            raise KeyError

    def __setitem__(self, key, val):
        if type(val) == type({}):
            val[self.name_to_use_as_key] = key
            if self.collection.find_one({self.name_to_use_as_key: key}):
                self.collection.replace_one({self.name_to_use_as_key: key}, val)
            else:
                self.collection.insert(val)
        else:
            raise TypeError

    def __len__(self):
        return self.collection.count()

