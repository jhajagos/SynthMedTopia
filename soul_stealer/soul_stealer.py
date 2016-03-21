
import csv
import os
import pprint
import random

"""
This is some exploratory code to look at how
"""


def main(csv_file_name, list_of_rules, number_of_samples):

    rules_number_dict = {}
    number_rules_dict = {}
    fields_to_capture_dict = {}
    number_fields_to_caputre_dict = {}
    rules_fields_count_dict = {}

    # Setup dictionaries that are going to be used
    i = 0
    for rule in list_of_rules:
        rules_number_dict[rule] = i
        number_rules_dict[i] = rule

        field_list = []
        for item in rule:
            if item is not None:
                if item.__class__ == ().__class__:
                    for subitem in item:
                        field_list += [subitem]
                else:
                    field_list += [item]
        tuple_field_list = tuple(field_list)
        fields_to_capture_dict[i] = tuple_field_list
        rules_fields_count_dict[tuple_field_list] = {}
        number_fields_to_caputre_dict[tuple_field_list] = i

        i += 1

    #pprint.pprint(rules_number_dict)
    #pprint.pprint(number_rules_dict)
    #pprint.pprint(fields_to_capture_dict)

    # For each combination of a field values generate a count
    with open(csv_file_name, "r") as f:
        csv_dict = csv.DictReader(f)
        for row_dict in csv_dict:
            for field_tuple in rules_fields_count_dict:
                captured_field_values = []
                for field in field_tuple:
                    captured_field_values += [row_dict[field]]
                captured_field_tuple = tuple(captured_field_values)
                if captured_field_tuple in rules_fields_count_dict[field_tuple]:
                    rules_fields_count_dict[field_tuple][captured_field_tuple] += 1
                else:
                    rules_fields_count_dict[field_tuple][captured_field_tuple] = 1

    # Build the functional relationships between the first part of a rule and the last
    function_value_dict = {}
    for field_tuple in rules_fields_count_dict:
        function_value_dict[field_tuple] = {}
        if len(field_tuple) > 1:
            # Build nested subdictionaries
            for field_value_tuple in rules_fields_count_dict[field_tuple]:
                mapping_count = rules_fields_count_dict[field_tuple][field_value_tuple]
                to_be_mapped = field_value_tuple[:-1]
                mapped_to = field_value_tuple[-1]

                if to_be_mapped not in function_value_dict[field_tuple]:
                    function_value_dict[field_tuple][to_be_mapped] = {mapped_to: mapping_count}
                else:
                    function_value_dict[field_tuple][to_be_mapped][mapped_to] = mapping_count
        else:
            function_value_dict[field_tuple] = rules_fields_count_dict[field_tuple]

    # Create a list of counts to be sorted
    function_value_dict_list = {}
    for field_tuple in function_value_dict:
        if len(field_tuple) == 1:
            function_value_dict_list[field_tuple] = []
            for function_value in function_value_dict[field_tuple]:
                function_value_dict_list[field_tuple] += [(function_value, function_value_dict[field_tuple][function_value])]
        else:
            function_value_dict_list[field_tuple] = {}
            for sub_field_tuple in function_value_dict[field_tuple]:
                function_value_dict_list[field_tuple][sub_field_tuple] = []
                for sub_function_value in function_value_dict[field_tuple][sub_field_tuple]:
                    function_value_dict_list[field_tuple][sub_field_tuple] += [(sub_function_value, function_value_dict[field_tuple][sub_field_tuple][sub_function_value])]


   # reverse sort the list so more frequent items occur first
    for field_tuple in function_value_dict_list:
        if len(field_tuple) == 1:
            function_value_dict_list[field_tuple].sort(key=lambda x: x[1], reverse=True)
        else:
            for sub_field_tuple in  function_value_dict[field_tuple]:
                function_value_dict_list[field_tuple][sub_field_tuple].sort(key=lambda x: x[1], reverse=True)

    # Compute probability ranges for random sampling
    function_value_dict_plist = {}
    for field_tuple in function_value_dict_list:
        if len(field_tuple) == 1:
            function_value_dict_plist[field_tuple] = []
            total_count = sum([xs[1] for xs in function_value_dict_list[field_tuple]]) * 1.0
            last_prob = 0.0
            for item in function_value_dict_list[field_tuple]:
                item_count = item[1] * 1.0
                class_prob = item_count /total_count
                function_value_dict_plist[field_tuple] += [((last_prob, class_prob + last_prob), item[0])]
                last_prob += class_prob
        else:
            function_value_dict_plist[field_tuple] = {}
            for sub_field_tuple in function_value_dict_list[field_tuple]:
                function_value_dict_plist[field_tuple][sub_field_tuple] = []
                total_count = sum([xs[1] for xs in function_value_dict_list[field_tuple][sub_field_tuple]]) * 1.0
                last_prob = 0.0
                for item in function_value_dict_list[field_tuple][sub_field_tuple]:
                    item_count = item[1] * 1.0
                    class_prob = item_count /total_count
                    function_value_dict_plist[field_tuple][sub_field_tuple] += [((last_prob, class_prob + last_prob), item[0])]
                    last_prob += class_prob


    # Now it is time to make copies of the soul

    data_list = []
    for i in xrange(0, number_of_samples):
        generated_values = {}

        failed = 0
        for rule in list_of_rules:

            if not failed:
                rule_number = rules_number_dict[rule]
                target_field = rule[-1]
                starting_rule = rule[0]
                field_tuple = fields_to_capture_dict[rule_number]

                if starting_rule is None: # These are independent variables
                    list_to_search = function_value_dict_plist[field_tuple]
                    random_number = random.uniform(0,1)

                    j = 0
                    while j < len(list_to_search):
                        probability_range = list_to_search[j][0]
                        if random_number >= probability_range[0] and random_number < probability_range[1]:
                            item = list_to_search[j][1]
                            break
                        j += 1

                    generated_values[target_field] = item[0]

                else:  # Variables which depend on other variables being set
                    field_list_value = []
                    for field in field_tuple[:-1]:
                        field_list_value += [generated_values[field]]
                    field_tuple_value = tuple(field_list_value)

                    if field_tuple_value in function_value_dict_plist[field_tuple]:
                        list_to_search = function_value_dict_plist[field_tuple][field_tuple_value]

                        random_number = random.uniform(0,1)

                        j = 0
                        while j < len(list_to_search):
                            probability_range = list_to_search[j][0]
                            if random_number >= probability_range[0] and random_number < probability_range[1]:
                                item = list_to_search[j][1]
                                break
                            j += 1
                        generated_values[target_field] = item
                    else:
                        failed = 1
        if not failed:
            data_list += [generated_values]

    pprint.pprint(data_list)


if __name__ == "__main__":
    list_of_rules = [
        (None, "Zip Code - 3 digits"),
        (None, "Gender"),
        ("Gender", "Age Group"),
        (("Age Group", "Gender"), "CCS DX"),
        ("CCS DX", "Length of Stay"),
        ("CCS DX", "CCS Proc")
    ]

    main("../../../data/sparcs/ccs_age_diagnosis_relationship.csv", list_of_rules, 20)