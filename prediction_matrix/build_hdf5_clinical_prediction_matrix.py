__author__ = 'janos'

import json
import h5py
import numpy as np
import pprint


def get_entry_from_path(dict_with_path, path_list):

    while len(path_list):
        key = path_list[0]
        path_list = path_list[1:]

        if key in dict_with_path:
            dict_with_path = dict_with_path[key]
        else:
            return None

    return dict_with_path


def expand_template_dict(data_dict, template_list_dict):
    new_templates = []

    for template_dict in template_list_dict:
        template_type = template_dict["type"]
        path = template_dict["path"]
        if template_type == "classes_templates":
            entry_classes = []
            for data_key in data_dict:
                data = data_dict[data_key]
                entries = get_entry_from_path(data, path)
                for key in entries:
                    if key not in entry_classes:
                        entry_classes += [key]
            template_class = template_dict["class_template"]
            entry_classes_dict = []
            for entry in entry_classes:
                new_template_dict = template_class.copy()
                new_template_dict["name"] = entry
                entry_classes_dict += [new_template_dict]

            new_type = template_dict["class_type"]
            new_template_dict = {"path": path, "type": new_type, new_type: entry_classes_dict}

            new_templates += [new_template_dict]

    template_list_dict += new_templates
    return template_list_dict


def build_translation_dict(data_dict, template_list_dict):
    data_translate_dict = {}
    for template_dict in template_list_dict:
        template_type = template_dict["type"]
        path = template_dict["path"]

        if template_type == "variables":
            variables_dict = template_dict[template_type]
            for variable_dict in variables_dict:
                item_dict = {}
                variable_type = variable_dict["type"]
                if variable_type == "categorical":
                    for data_key in data_dict:
                        datum_dict = data_dict[data_key]
                        dict_of_interest = get_entry_from_path(datum_dict, path)
                        if variable_dict["cell_value"] in dict_of_interest:
                            value_of_interest = str(dict_of_interest[variable_dict["cell_value"]])
                            if value_of_interest in item_dict:
                                item_dict[value_of_interest] += 1
                            else:
                                item_dict[value_of_interest] = 1

                    data_keys = item_dict.keys()
                    data_keys.sort()

                    position_map = {}
                    for i in range(len(data_keys)):
                        position_map[data_keys[i]] = i
                    variable_dict["position_map"] = position_map

        elif template_type == "categorical_list":
            item_dict = {}
            for data_key in data_dict:
                datum_dict = data_dict[data_key]
                dicts_of_interest = get_entry_from_path(datum_dict, path)

                for dict_of_interest in dicts_of_interest:

                    if template_dict["field"] in dict_of_interest:
                        value_of_interest = str(dict_of_interest[template_dict["field"]])
                        if value_of_interest in item_dict:
                            item_dict[value_of_interest] += 1
                        else:
                            item_dict[value_of_interest] = 1

            data_keys = item_dict.keys()
            data_keys.sort()

            position_map = {}
            for i in range(len(data_keys)):
                position_map[data_keys[i]] = i

            template_dict["position_map"] = position_map


    return template_list_dict

def build_hdf5_matrix(hdf5p, data_dict, data_translate_dict):
    pass

def main(hdf5_file_name, data_json_file, data_template_json):
    with open(data_json_file, "r") as f:
        data_dict = json.load(f)
        #pprint.pprint(data_dict)

    with open(data_template_json, "r") as f:
        data_template_dict = json.load(f)
        #pprint.pprint(data_template_dict)

    data_template_dict = expand_template_dict(data_dict, data_template_dict)
    data_translate_dict = build_translation_dict(data_dict, data_template_dict)

    pprint.pprint(data_translate_dict)

    f5p = h5py.File(hdf5_file_name, "w")

    build_hdf5_matrix(f5p, data_dict, data_translate_dict)


if __name__ == "__main__":
    main("matrix_build.hdf5", "fake_inpatient_readmission_data.json", "configuration_to_build_matrix.json")