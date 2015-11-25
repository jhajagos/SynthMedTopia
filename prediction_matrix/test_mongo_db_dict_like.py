__author__ = 'janos'

import unittest

from mongodb_dict_like import DictMappingMongo
from pymongo import MongoClient

class TestDictMapping(unittest.TestCase):

    def setUp(self):
        self.client = MongoClient()
        self.collection = "unittest"
        self.database = "testing_py"
        self.field_name = "transaction_id"

        self.client[self.database][self.collection].remove()

    def test_class(self):
        test_collection = DictMappingMongo(self.client, self.database, self.collection, self.field_name)
        n = len(test_collection)
        self.assertEqual(n, 0)

        test_collection["12345"] = {"name": "hello"}

        n1 = len(test_collection)
        self.assertEqual(n1, 1)

        item_dict = test_collection["12345"]
        self.assertIsNotNone(item_dict)

        test_collection["12345"] = {"name": "sparkle"}

        n2 = len(test_collection)
        self.assertEqual(n2, 1)

        try:
            test_collection["123"]
        except KeyError:
            self.assertTrue(1)

        test_collection["54321"] = {"name": "twinkle"}

        n3 = len(test_collection)
        self.assertEqual(n3, 2)

        try:
            test_collection["66666"] = 4
        except TypeError:

            self.assertTrue(1)

        keys = []
        for key in test_collection:
            keys += [key]

        self.assertTrue("54321" in keys)
        self.assertTrue("12345" in keys)
        self.assertFalse("123" in keys)

        self.assertEquals(len(keys), 2)

if __name__ == '__main__':
    unittest.main()
