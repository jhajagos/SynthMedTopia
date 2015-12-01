__author__ = 'janos'

import unittest
import build_hdf5_matrix_from_document as bhm

class RunHDF5Mapping(unittest.TestCase):
    def test_mapping(self):

        bhm.main("transaction_test.hdf5", "fake_inpatient_readmission_data.json", "configuration_to_build_matrix.json")

        #TODO Add sort order key

        self.assertEqual(True, False)



if __name__ == '__main__':
    unittest.main()
