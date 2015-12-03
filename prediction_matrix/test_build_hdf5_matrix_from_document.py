__author__ = 'janos'

import unittest
import h5py
import build_hdf5_matrix_from_document as bhm
import numpy as np

class RunHDF5Mapping(unittest.TestCase):
    def test_mapping(self):

        bhm.main("transaction_test.hdf5", "fake_inpatient_readmission_data.json", "configuration_to_build_matrix.json")
        "transaction_test.hdf5"
        f5 = h5py.File("transaction_test.hdf5",'r')
        dca = f5["/independent/classes/discharge/core_array"][...]
        self.assertEquals(dca.shape, (2, 3))

        dcac = f5["/independent/classes/discharge/column_annotations"][...]
        self.assertEquals(dcac.shape, (3, 3))

        lab_count = f5["/independent/classes/lab/count/core_array"][...]
        lab_count_c = f5["/independent/classes/lab/count/column_annotations"][...]

        self.assertEqual(lab_count.tolist(), [[4.,  0.], [4.,  1.]])
        self.assertEqual(lab_count_c.tolist(), [['BUN', 'Troponin'], ['value', 'value'], ['count', 'count']])

        lab_category_count = f5["/independent/classes/lab/category/core_array"][...]
        lab_category_count_c = f5["/independent/classes/lab/category/column_annotations"][...]

        self.assertEqual(lab_category_count.tolist(), [[4.,  0.,  0.,  0.], [ 1.,  1.,  2.,  1.]])
        self.assertEqual(lab_category_count_c.tolist(), [['BUN', 'BUN', 'BUN', 'Troponin'],
                                                         ['high', 'low', 'normal', 'extreme'],
                                                         ['', '', '', '']])

        self.assertEquals(np.sum(lab_category_count), np.sum(lab_count), "Sums should be equal")

if __name__ == '__main__':
    unittest.main()
