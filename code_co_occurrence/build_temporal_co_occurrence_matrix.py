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
    /overall/co_occur_same_day_matrix/ - estimate a symmetric matrix/
    /overall/co_occur_not_same_day_matrix/
    /overall/annotations/row
    /overall/annotations/column

    attributes:
        n_entities
        n_entity_name
        n_records
        n_transactions
        n_transaction_name

        lag +-

"""


def generate_co_occurrence_matrices(config, h5p, connection, path, forward_code_map_dict, window=180, dimension=None,
                                    dimension_values_dict=None):
    """
    :param config:
    :param h5p:
    :param connection:
    :param window:
    :param dimension:
    :param dimension_values_dict:
    :return:
    """

    # Estimate number of entities

    entity_id = config["entity_id"]
    table_name = config["table_name"]
    transaction_id = config["transaction_id"]

    query_base_count = 'select count(distinct %s) as n_entities, count(distinct %s), count(*) as n_records' % (entity_id, transaction_id)
    query_count = query_base_count + ' from %s ' % table_name
    condition_clause = ""
    if dimension is not None or dimension_values_dict is not None:
        condition_clause += " where "

    if dimension is not None:
        condition_clause += '%s = ?'

    if dimension_values_dict is not None:
        if condition_clause != " where ":
            condition_clause += " and "
        dimension_values = []
        for key in dimension_values_dict:
            dimension_values += dimension_values_dict[key]
            condition_clause += "%s = ?" % key

    query_count += condition_clause
    n_entities, n_transactions, n_records = list(connection.execute(query_count))[0]

    # Diagonal - entities of code class
    code_field_name = config["code_field_name"]
    query_group_count = query_base_count + ', %s as code' % code_field_name
    query_group_count += " from %s " % table_name
    query_group_count += condition_clause
    query_group_count += " group by %s" % code_field_name

    group_counts = list(connection.execute(query_group_count))

    n_codes = len(forward_code_map_dict)
    overall_co = np.zeros(shape=(n_codes, n_codes),dtype='uint32')

    for group_count in group_counts:
        position_i = forward_code_map_dict[group_count["code"]]
        overall_co[position_i, position_i] = group_count["n_entities"]

    #print(group_counts)

    # Overall connections
    core_overall_base = """select dd1.%s as code1, dd2.%s as code2, count(distinct dd1.%s) as n_entities
 from %s dd1 join %s dd2
  on (dd1.%s = dd2.%s and dd1.%s != dd2.%s)
  group by dd1.%s, dd2.%s""" % \
                        (code_field_name, code_field_name, entity_id, table_name, table_name, entity_id, entity_id,
                         code_field_name, code_field_name, code_field_name, code_field_name)

    result_cores = connection.execute(core_overall_base)

    for result_core in result_cores:
        i = forward_code_map_dict[result_core[0]]
        j = forward_code_map_dict[result_core[1]]
        overall_co[i,j] = result_core[2]



    core_array_ds = h5p.create_dataset(path + "co_occur/", shape=(n_codes, n_codes), dtype="uint32")
    core_array_ds[...] = overall_co





    # Lower Quadrant - code 1 occurs before code 2

    # Upper Quadrant - code 2 occurs after code 1

    # Every matrix is the same size


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

    # Store row and column annotations

    max_field_size = 0
    for code,code_desc in code_list:
        if len(code) > max_field_size:
            max_field_size = len(code)

        if len(code_desc) > max_field_size:
            max_field_size = len(code_desc)


    row_annot_d = hf5.create_dataset('/overall/annotations/row/', shape=(len(code_list), 2), dtype="S" + str(max_field_size))
    col_annot_d = hf5.create_dataset('/overall/annotations/column/', shape=(2, len(code_list)), dtype="S" + str(max_field_size))

    # Overall

    row_annot_array = np.array(code_list, dtype="S" + str(max_field_size))
    row_annot_d[...] = row_annot_array

    col_annot_array = row_annot_array.transpose()
    col_annot_d[...] = col_annot_array

    # Dimensions Overall
    entity_id = config["entity_id"]
    generate_co_occurrence_matrices(config, hf5, engine, "/overall/", code_forward_dict)


    # Cross Dimensions


def read_configuration(json_file_name):
    with open(json_file_name, "r") as f:
        config = json.load(f)
    return config


if __name__ == "__main__":
    main()