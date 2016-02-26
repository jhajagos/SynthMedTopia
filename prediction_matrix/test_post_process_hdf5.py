import unittest
import h5py
import os
import numpy

class TestNormalizeCounts(unittest.TestCase):
    def setUp(self):
        if os.path.exists("./test/test_hdf5.hdf5"):
            os.remove("./test/test_hdf5.hdf5")

        f5=h5py.File("./test/test_hdf5.hdf5", "w")
        ca = f5.create_dataset("/lab/categories/column_annotations", dtype="S128", shape=(3,7))
        lab_tests = [["Hemoglobin","Hemoglobin", "Blood Glucose", "Blood Glucose", "Blood Glucose", "WBC", "WBC"],
                     ["Normal", "Abnormal", "Low", "Normal", "High", "Normal", "Abnormal"],
                     ["", "", "", "", "", "", ""]]
        lab_test_array = numpy.array(lab_tests, dtype="S128")
        ca[...] = lab_test_array

        lab_test_values = [[0, 1, 5, 2, 1, 10, 1], [0, 0, 0, 0, 0, 0, 1], [5, 0, 0, 10, 0, 15, 1]]

        lab_test_corearray = numpy.array(lab_test_values)
        corea = f5.create_dataset("/lab/categories/core_array", shape=(3,7))
        corea[...] = lab_test_corearray

    def test_something(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
