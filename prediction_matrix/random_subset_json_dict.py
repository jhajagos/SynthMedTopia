"""
For testing generate a random subset of json file that has random keys
"""

__author__ = 'janos'

import random
import sys
import json
import os


def main(json_dict_file_name, n_samples):

    print("Reading '%s'" % os.path.abspath(json_dict_file_name))
    with open(json_dict_file_name, "r") as fj:
        dict_to_samples = json.load(fj)

    key_list = dict_to_samples.keys()
    len_key_list = len(key_list)


    print("Creating %s samples" % n_samples)
    sampled_dict = {}
    i = 0
    while i < n_samples:
        selected_key_i = random.randint(0, len_key_list - 1)
        selected_key = key_list[selected_key_i]

        if selected_key not in sampled_dict:
            sampled_dict[selected_key] = dict_to_samples[selected_key]
            i += 1
    sampled_json_dict_file_name = json_dict_file_name + ".sampled." + str(n_samples) + ".json"

    print("Writing '%s'" % os.path.abspath(sampled_json_dict_file_name))
    with open(sampled_json_dict_file_name, "w") as fwj:
        json.dump(sampled_dict, fwj, sort_keys=True, indent=4, separators=(',',': '))

if __name__ == "__main__":
    main(sys.argv[1], int(sys.arv[2]))