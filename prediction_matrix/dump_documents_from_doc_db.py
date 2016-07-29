"""
This program extracts a collection from a MongoDB instance and formats in a way the
pipeline works.
"""

[
    {
        "batch_id": 1,
        "data_json_file": "C:\\Users\\janos\\GitHub\\SynthMedTopia\\prediction_matrix\\test\\transactions_split_1_20160323.json",
        "sort_order_file_name": "C:\\Users\\janos\\GitHub\\SynthMedTopia\\prediction_matrix\\test\\transactions_split_1_key_order_20160323.json"
    },
    {
        "batch_id": 2,
        "data_json_file": "C:\\Users\\janos\\GitHub\\SynthMedTopia\\prediction_matrix\\test\\transactions_split_2_20160323.json",
        "sort_order_file_name": "C:\\Users\\janos\\GitHub\\SynthMedTopia\\prediction_matrix\\test\\transactions_split_2_key_order_20160323.json"
    }
]

import pymongo
import os
import json
import gzip

# def data_dict_load(data_dict_json_file_name):
#
#     if data_dict_json_file_name[-2:] == "gz":
#         with gzip.open(data_dict_json_file_name, "rb") as f:
#             data_dict = json.loads(f.read().decode("ascii"))
#     else:
#         with open(data_dict_json_file_name, "rb") as fj:
#             data_dict = json.load(fj)
#
#     return data_dict


def write_keyed_json_file(base_directory, base_name, nth_file, file_batches_dict, query_results_dict, key_orders):
    json_file_name = os.path.join(base_directory, base_name + "_" + str(nth_file) + ".json")
    sort_order_file_name = os.path.join(base_directory, base_name + "_" + str(nth_file) + "_key_order.json")
    file_batches_dict += [{"batch_id": nth_file, "data_json_file": json_file_name,
                           "sort_order_name": sort_order_file_name}]

    with open(json_file_name, "w") as fw:
        json.dump(query_results_dict, fw)

    with open(sort_order_file_name, "w") as fw:
        json.dump(key_orders, fw)

    return file_batches_dict


def main(query_to_run, base_directory, base_name, runtime_config, size_of_batches=1000):

    connection_string = runtime_config["connection_string"]
    database_name = runtime_config["database_name"]
    client = pymongo.MongoClient(connection_string)
    collection_name = runtime_config["collection_name"]

    database = client[database_name]
    collection = database[collection_name]

    cursor = collection.find(query_to_run)

    i = 0
    j = 0

    file_batches_dict = []
    query_results_dict = {}
    key_orders = []

    for row_dict in cursor:

        row_dict.pop("_id", None)

        query_results_dict[i] = row_dict
        key_orders += [i]

        if i > 0 and i % size_of_batches == 0:

            j += 1
            file_batches_dict = write_keyed_json_file(base_directory, base_name, j, file_batches_dict,
                                                      query_results_dict, key_orders)
            key_orders = []
            query_results_dict = {}

        i += 1

    if len(query_results_dict) > 0:
        j += 1
        file_batches_dict = write_keyed_json_file(base_directory, base_name, j, file_batches_dict, query_results_dict,
                                                  key_orders)

    batch_file_name = os.path.join(base_directory, base_name + "_batches.json")
    with open(batch_file_name, "w") as fw:
        json.dump(file_batches_dict, fw)

    return file_batches_dict



if __name__ == "__main__":
    pass