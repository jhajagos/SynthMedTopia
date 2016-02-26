import unittest
import json
import pprint

class TestDumpJSON(unittest.TestCase):

    def setUp(self):

        with open("./test/runtime_config_test.json", "r") as f:
            test_load = json.load(f)
            pprint.pprint(test_load)

            """
            u'mongo_db_config': {u'collection_name': u'mapped_encounters',
                      u'connection_string': u'mongodb://localhost',
                      u'database_name': u'encounters',

                """







    def test_something(self):
        self.assertEqual(True, False)



if __name__ == '__main__':
    unittest.main()
