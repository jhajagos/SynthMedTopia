__author__ = 'janos'

import csv
import random
import math

def logger(string_to_log=""):
    print(string_to_log)


def main(csv_file_name, columns_to_add_random_noise, sample_size=10, one_observation_per_person=True, identifier_name = "mrn"):
    """columns_to_add_random_noise = ["BP","blood temperature"]"""

    dict_values_pass_through = {}
    for column in columns_to_add_random_noise:
            dict_values_pass_through[column] = {"min": None, "max": None}

    with open(csv_file_name, "rU") as f:
        csv_dict_reader = csv.DictReader(f)
        n = 0
        logger("First pass through")
        for datum in csv_dict_reader:

            for column in columns_to_add_random_noise:
                if n == 0:
                    dict_values_pass_through[column]["min"] = float(datum[column])
                    dict_values_pass_through[column]["max"] = float(datum[column])
                else:
                    dict_values_pass_through[column]["min"] = min(dict_values_pass_through[column]["min"], float(datum[column]))
                    dict_values_pass_through[column]["max"] = max(dict_values_pass_through[column]["max"], float(datum[column]))
                dict_values_pass_through[column]

            n += 1

    logger("File has %s rows" % n)

    random_sample = []
    for r in range(sample_size):
        random_sample += [random.randint(0, n)]

    logger("Subsetting dataset")
    data_subset = []
    with open(csv_file_name, "rU") as f:
        csv_dict_reader = csv.DictReader(f)
        nr = 0
        logger("Second pass through")
        for datum in csv_dict_reader:
            if nr in random_sample:
                datum["__row__"] = nr
                data_subset += [datum]
            nr += 1

    altered_subset_data = []
    for subset_datum in data_subset:

        for column in columns_to_add_random_noise:
            min_value = dict_values_pass_through[column]["min"]
            max_value = dict_values_pass_through[column]["max"]

            range_value = max_value - min_value
            range_lower = math.floor(0.05 * range_value)
            range_upper = math.floor(0.05 * range_value)

            change = random.randint(range_lower, range_upper)
            direction = random.randint(-1, 1)

            value_to_change = float(subset_datum[column])
            changed_value = value_to_change + (change * direction)

            subset_datum[column + " altered"] = changed_value
        altered_subset_data += [subset_datum]

    logger("Writing file")

    with open(csv_file_name, "rU") as f:
        csvr = csv.reader(f)
        header = csvr.next()

    with open(csv_file_name + ".subsetted_random.csv", "w") as fw:

        csv_writer = csv.writer(fw)
        new_columns_With_noise = []
        for column in columns_to_add_random_noise:
            new_columns_With_noise += [column + " altered"]

        new_header = header + new_columns_With_noise

        csv_writer.writerow(new_header)

        for altered_subset_datum in altered_subset_data:
            new_row = []
            for column in new_header:
                new_row += [altered_subset_datum[column]]

            csv_writer.writerow(new_row)

if __name__ == "__main__":
    main("/Users/janos/Downloads/cardiac_risk_eval.csv",["Age years","Total Cholesterol", "HDL-Cholesterol", "Systolic Blood Pressure"], 1000)