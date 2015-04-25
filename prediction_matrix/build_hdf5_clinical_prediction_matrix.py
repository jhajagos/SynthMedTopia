__author__ = 'janos'

import json
import h5py
import numpy as np
import pprint
import sys


def get_entry_from_path(dict_with_path, path_list):
    """Traverses a path list in a dict of dicts"""
    while len(path_list):
        key = path_list[0]
        path_list = path_list[1:]

        if key in dict_with_path:
            dict_with_path = dict_with_path[key]
        else:
            return None

    return dict_with_path


def generate_column_annotations_categorical_list(categorical_list_dict, column_annotations):
    """Generate column annotations when it is a column list"""
    position_map = categorical_list_dict["position_map"]
    name = categorical_list_dict["name"]
    descriptions = categorical_list_dict["descriptions"]

    position_map_reverse = {}
    for k in position_map:
        v = position_map[k]
        position_map_reverse[v] = k

    for i in range(categorical_list_dict["n_categories"]):
        value = position_map_reverse[i]
        if value in descriptions:
            description = descriptions[value]
        else:
            description = ""

        column_annotations[0, i] = name.encode("ascii", errors="replace")
        column_annotations[1, i] = value.encode("ascii", errors="replace")
        column_annotations[2, i] = description.encode("ascii", errors="replace")

    return column_annotations


def generate_column_annotations_variables(variables_dict, column_annotations):
    """Generate column annotations based on variables"""

    for variable_dict in variables_dict["variables"]:
        offset_start = variable_dict["offset_start"]
        variable_type = variable_dict["type"]
        if variable_type == "categorical":
            position_map = variable_dict["position_map"]

            position_map_reverse = {}
            for k in position_map:
                v = position_map[k]
                position_map_reverse[v] = k

            for i in range(len(position_map.keys())):

                value = position_map_reverse[i]

                field_value = variable_dict["cell_value"]

                descriptions = variable_dict["descriptions"]
                if value in descriptions:
                    description = descriptions[value]
                else:
                    description = ""

                column_annotations[0, offset_start + i] = field_value
                column_annotations[1, offset_start + i] = value
                column_annotations[2, offset_start + i] = description
        else:
            field_name = variable_dict["cell_value"]

            if variable_type == "numeric_list":
                column_annotations[0, offset_start ] = variable_dict["name"]
                column_annotations[1, offset_start] = field_name
                column_annotations[2, offset_start] = variable_dict["process"]
            else:
                column_annotations[0, offset_start] = field_name

    return column_annotations

def expand_template_dict(data_dict, template_list_dict):
    """Expand out to more detail based on encoded data in a data_dict"""
    new_templates = []

    for template_dict in template_list_dict:
        template_type = template_dict["type"]
        path = template_dict["path"]
        if template_type == "classes_templates":
            entry_classes = []
            for data_key in data_dict:
                data = data_dict[data_key]
                entries = get_entry_from_path(data, path)
                if entries is not None:
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


def add_offsets_to_translation_dict(template_list_dict):
    """Add offsets to the dicts so as we know where variables start and end"""

    for template_dict in template_list_dict:

        template_type = template_dict["type"]

        if template_type == "variables":
            variable_dicts = template_dict["variables"]
            offset_start = 0

            for variable_dict in variable_dicts:

                variable_type = variable_dict["type"]
                if variable_type == "categorical":
                    n_categories = variable_dict["n_categories"]
                    offset_end = offset_start + n_categories
                else:
                    offset_end = offset_start + 1

                variable_dict["offset_start"] = offset_start
                variable_dict["offset_end"] = offset_end

                offset_start = variable_dict["offset_end"]

        elif template_type == "categorical_list":
            n_categories = template_dict["n_categories"]
            offset_end = n_categories

            template_dict["offset_start"] = 0
            template_dict["offset_end"] = offset_end

    return template_list_dict


def build_translation_dict(data_dict, template_list_dict):
    """For categorical variables build lookup dicts"""
    for template_dict in template_list_dict:
        template_type = template_dict["type"]
        path = template_dict["path"]

        if template_type == "variables":
            variables_dict = template_dict[template_type]
            for variable_dict in variables_dict:
                item_dict = {}
                variable_type = variable_dict["type"]
                if variable_type == "categorical":
                    description_dict = {}
                    label_dict = {}
                    for data_key in data_dict:
                        datum_dict = data_dict[data_key]
                        dict_of_interest = get_entry_from_path(datum_dict, path)
                        if variable_dict["cell_value"] in dict_of_interest:
                            value_of_interest = str(dict_of_interest[variable_dict["cell_value"]])
                            if value_of_interest in item_dict:
                                item_dict[value_of_interest] += 1
                            else:
                                item_dict[value_of_interest] = 1
                                if "description" in variable_dict:
                                    description_dict[value_of_interest] = dict_of_interest[variable_dict["description"]]
                                if "label" in variable_dict:
                                    label_dict[value_of_interest] = dict_of_interest[variable_dict["label"]]

                    data_keys = item_dict.keys()
                    data_keys.sort()

                    position_map = {}
                    for i in range(len(data_keys)):
                        position_map[data_keys[i]] = i
                    variable_dict["position_map"] = position_map
                    variable_dict["labels"] = label_dict
                    variable_dict["descriptions"] = description_dict
                    variable_dict["n_categories"] = len(position_map.keys())

        elif template_type == "categorical_list":

            description_dict = {}
            label_dict = {}
            item_dict = {}

            for data_key in data_dict:
                datum_dict = data_dict[data_key]
                dicts_of_interest = get_entry_from_path(datum_dict, path)
                if dicts_of_interest is not None:
                    for dict_of_interest in dicts_of_interest:

                        if template_dict["field"] in dict_of_interest:
                            value_of_interest = str(dict_of_interest[template_dict["field"]])
                            if value_of_interest in item_dict:
                                item_dict[value_of_interest] += 1
                            else:
                                item_dict[value_of_interest] = 1
                                if "description" in template_dict:
                                    description_dict[value_of_interest] = dict_of_interest[template_dict["description"]]
                                if "label" in template_dict:
                                    label_dict[value_of_interest] = dict_of_interest[template_dict["label"]]

            data_keys = item_dict.keys()
            data_keys.sort()

            position_map = {}
            for i in range(len(data_keys)):
                position_map[data_keys[i]] = i

            template_dict["position_map"] = position_map
            template_dict["labels"] = label_dict
            template_dict["descriptions"] = description_dict
            template_dict["n_categories"] = len(position_map.keys())

    return template_list_dict


def build_hdf5_matrix(hdf5p, data_dict, data_translate_dict_list):
    """Each class will be a dataset in a hdf5 matrix"""

    data_items_count = len(data_dict.keys())

    for data_translate_dict in data_translate_dict_list:
        template_type = data_translate_dict["type"]

        if "export_path" in data_translate_dict:
            export_path = data_translate_dict["export_path"]
        else:
            export_path = data_translate_dict["path"]

        path = data_translate_dict["path"]

        hdf5_base_path = "/".join(export_path)
        hdf5_core_array_path = "/" + hdf5_base_path + "/core_array/"

        if template_type == "variables":
            offset_end = data_translate_dict["variables"][-1]["offset_end"]

        if template_type == "categorical_list":
            offset_end = data_translate_dict["offset_end"]

        if template_type in ("variables", "categorical_list"):
            core_array = np.zeros(shape=(data_items_count, offset_end))
            column_annotations = np.zeros(shape=(3, offset_end), dtype="S64")

        if template_type == "variables":

            for variable_dict in data_translate_dict["variables"]:
                offset_start = variable_dict["offset_start"]
                variable_type = variable_dict["type"]
                cell_value_field = variable_dict["cell_value"]

                if variable_type == "categorical":
                    position_map = variable_dict["position_map"]
                    i = 0
                    for data_key in data_dict:
                        datum_dict = data_dict[data_key]
                        dict_of_interest = get_entry_from_path(datum_dict, path)

                        if cell_value_field in dict_of_interest:
                            field_value = str(dict_of_interest[cell_value_field])
                            field_value_position = position_map[field_value]
                            core_array[i, offset_start + field_value_position] = 1
                        i += 1
                else:
                    i = 0

                    if "process" in variable_dict:
                        process = variable_dict["process"]
                        variable_name = variable_dict["name"]

                    else:
                        process = None
                        variable_name = None

                    for data_key in data_dict:
                        datum_dict = data_dict[data_key]
                        dict_of_interest = get_entry_from_path(datum_dict, path)
                        if dict_of_interest is not None:

                            if (variable_name in dict_of_interest) or (dict_of_interest.__class__ == [].__class__):
                                if dict_of_interest.__class__ == {}.__class__:
                                    list_of_interest = dict_of_interest[variable_name]
                                else:
                                    list_of_interest = dict_of_interest

                                if list_of_interest is not None:
                                    if variable_type == 'numeric_list':

                                        if process == "median":
                                            process_list = []
                                            for item in list_of_interest:
                                                if cell_value_field in item:
                                                    cell_value = item[cell_value_field]
                                                    if cell_value is not None:
                                                        process_list += [cell_value]
                                            process_array = np.array(process_list)
                                            median_value = np.median(process_array)
                                            core_array[i, offset_start] = median_value

                                        elif process == "last_item":
                                            process_list = []
                                            for item in list_of_interest:
                                                if cell_value_field in item:
                                                    process_list += [item[cell_value_field]]
                                            core_array[i, offset_start] = process_list[-1]

                            if cell_value_field in dict_of_interest:
                                field_value = dict_of_interest[cell_value_field]
                                core_array[i, offset_start] = field_value
                        i += 1

        elif template_type == "categorical_list":
            cell_value_field = data_translate_dict["cell_value"]
            position_map = data_translate_dict["position_map"]
            i = 0
            for data_key in data_dict:
                datum_dict = data_dict[data_key]
                dict_of_interest = get_entry_from_path(datum_dict, path)

                if dict_of_interest is not None:
                    j = 0
                    for item_dict in dict_of_interest:
                        if cell_value_field in item_dict:
                            field_value = item_dict[cell_value_field]
                            position = position_map[str(field_value)]
                            if core_array[i, position] == 0:
                                core_array[i, position] = j + 1
                        j += 1
                i += 1

        if template_type in ("variables", "categorical_list"):

            hdf5_column_annotation_path = "/" + hdf5_base_path + "/column_annotations/"

            core_data_set = hdf5p.create_dataset(hdf5_core_array_path, shape=(data_items_count, offset_end))
            core_data_set[...] = core_array

            print("\n***************************")

            print(hdf5_core_array_path, hdf5_column_annotation_path)
            print("Core array:")
            print(core_array)

            column_header_path = "/" + hdf5_base_path + "/column_header/"
            column_header_set = hdf5p.create_dataset(column_header_path, shape=(3,), dtype="S16")
            column_header_set[...] = np.array(["field_name", "value", "description"])
            print("\nAnnotations:")
            print(np.transpose(column_header_set[...]))

            if template_type == "variables":
                column_annotations = generate_column_annotations_variables(data_translate_dict, column_annotations)
                print(column_annotations)

            elif template_type == "categorical_list":
                column_annotations = generate_column_annotations_categorical_list(data_translate_dict, column_annotations)

            print(column_annotations)

            print("***************************")

            column_data_set = hdf5p.create_dataset(hdf5_column_annotation_path, shape=(3, offset_end), dtype="S64")
            column_data_set[...] = column_annotations


def main(hdf5_file_name, data_json_file, data_template_json):
    """Convert a JSON file to a HDF5 matrix format using a template"""

    #Import JSON files
    with open(data_json_file, "r") as f:
        data_dict = json.load(f)

    with open(data_template_json, "r") as f:
        data_template_dict = json.load(f)

    data_template_dict = expand_template_dict(data_dict, data_template_dict)
    data_translate_dict = build_translation_dict(data_dict, data_template_dict)
    data_translate_dict = add_offsets_to_translation_dict(data_translate_dict)

    print("Generated data template:\n")
    pprint.pprint(data_translate_dict)
    f5p = h5py.File(hdf5_file_name, "w")

    build_hdf5_matrix(f5p, data_dict, data_translate_dict)


if __name__ == "__main__":

    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 2 and sys.argv[1] == "help":
        print("Usage: python build_hdf5_clinical_prediction_matrix.py output.hdf5 data_file.json data_template.json")
    else:
        main("matrix_build.hdf5", "fake_inpatient_readmission_data.json", "configuration_to_build_matrix.json")