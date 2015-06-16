__author__ = 'janos'

import json

"""
    Given a table in relational PostGreSQL database build co-occurrence matrix across multiple dimensions.

    HDF5 hierarchal file layout

    /dimension/
    /dimension/gender/values - dataset 1 d char array
    /dimension/gender/values - dataset 1 d char array
    /dimension/gender/M
    /dimension/gender/F
    /dimension/age/10
    /dimension/cross/gender/M/age/5

    /overall/

    /overall/co_occur_matrix
    /overall/annotations

    attributes
        n
        lag
        method

"""



def read_configuration(json_file_name):
    with open(json_file_name, "r") as f:
        config = json.load(f)
    return config




if __name__ == "__main__":
    pass