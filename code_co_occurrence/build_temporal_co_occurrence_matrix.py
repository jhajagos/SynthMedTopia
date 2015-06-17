__author__ = 'janos'

import json
import sqlalchemy as sa
import numpy as np
import h5py
import config_db
import os
from generate_null_model_data import define_number_map

"""
    Given a table in relational PostGreSQL database build co-occurrence matrix across multiple dimensions.

    HDF5 hierarchal file layout

    /dimension/
    /dimension/names - dataset 1 1D char array ['gender','age']
    /dimension/gender/values - dataset 1D char array
    /dimension/gender/values - dataset 1D char array
    /dimension/gender/M
    /dimension/gender/F
    /dimension/age/10
    /dimension/cross/gender/M/age/5

    /overall/
    /overall/co_occur_matrix/ - estimate a symmetric matrix - DX can occur in any order
    /overall/co_occur_temporal_ordering_matrix/
    /overall/co_occur_same_day_matrix - estimate a symmetric matrix/
    /overall/annotations/

    attributes
        n
        lag
        method

"""


def main(config_file_name="./configuration_matrix.json"):
    config = read_configuration(config_file_name)

    # Connect to a database
    engine = sa.create_engine(config_db.connection_url)

    # Get database metadata
    metadata = sa.MetaData(engine, reflect=True)

    # Delete HDF5 file if it exists

    hdf5_file_name = config["hdf5_file_name"]

    if os.path.exists(hdf5_file_name):
        os.remove(hdf5_file_name)

    # Open HDF5 file
    hf5 = h5py.File(hdf5_file_name, "w")

    # Get Codes and Code Labels

    table_name = config["table_name"]

    table_obj = metadata.tables[table_name]

    code_field_name = config["code_field_name"]
    code_field_description = config["code_field_description"]
    code_field_obj = table_obj.columns[code_field_name]
    code_desc_obj = table_obj.columns[code_field_description]
    code_set_query = sa.select([code_field_obj, code_desc_obj]).group_by(code_field_obj, code_desc_obj).order_by(code_field_obj)

    code_list = list(engine.execute(code_set_query))

    code_list_dict = {}

    for code in code_list:
        code_list_dict[code[0]] = code[1]

    # Build forward and reverse maps
    code_forward_dict, code_reverse_dict = define_number_map(code_list_dict)

    # Get Dimension Members
    dimension_fields = config["dimension_fields"]
    dimension_fields_obj = []

    for dimension_field in dimension_fields:
        dimension_fields_obj += [table_obj.columns[dimension_field]]

    dimension_values = []
    for dimension_field_obj in dimension_fields_obj:
        dimension_query_obj = sa.select([dimension_field_obj]).group_by(dimension_field_obj).order_by(dimension_field_obj)
        dimension_values += [[item[0] for item in list(engine.execute(dimension_query_obj))]]

    dimension_str_values = []
    for dimension_value in dimension_values:
        dimension_str_value = []
        for item in dimension_value:
            dimension_str_value += [str(item)]
        dimension_str_values += [dimension_str_value]

    # Add dimension names
    dimension_fields_str = [str(x) for x in dimension_fields]
    dim_names_ds = hf5.create_dataset("/dimensions/names/", shape=(1, len(dimension_fields_str)), dtype="S32")
    dim_names_ds[...] = np.array(dimension_fields_str)

    # Add dimensional values
    i = 0
    for dimension_field in dimension_fields:
        max_string_length = 0
        for dimension_str_value in dimension_str_values[i]:
            if len(dimension_str_value) > max_string_length:
                max_string_length = len(dimension_str_value)

        dim_values_ds = hf5.create_dataset('/dimensions/' + dimension_field + '/values/', shape=(1,len(dimension_str_values[i])),
                                                                                                 dtype="S" + str(max_string_length))

        dim_values_ds[...] = np.array(dimension_str_values[i])
        i += 1

    # Overall

    # Estimate number of entities

    # Diagonal - entities of code class

    # Lower Quadrant - code 1 occurs before code 2

    # Upper Quadrant - code 2 occurs after code 1

    # Every matrix is the same size

    # Dimensions Overall

    # Cross Dimensions


def read_configuration(json_file_name):
    with open(json_file_name, "r") as f:
        config = json.load(f)
    return config


if __name__ == "__main__":
    main()