__author__ = 'janos'

"""
    The goal is to generate a test data set with correct answers
    for estimating readmission/admission risk
"""

import datetime as dt
import csv
import numpy as np
import pprint

HOSPITALS = {1: "HA", 2: "HB", 3: "HC", 4: "HD", 5: "HE"}
PATIENTS = {1: "p1", 2: "p2", 3: "p3", 4: "p4", 5: "p5", 6: "p6", 7: "p7"}

CASES =[
    {
        "patient" : 1, "case": "Multiple hospitals with non overlapping inpatients",
         "pattern of visits": """
 11
         1111111
                          2222222
         """},
    {"patient": 2, "case": "No hospital visits", "pattern of visits": """



        """
    },
    {"patient": 3, "case": "One visit", "pattern of visits": """


                                                       55555555

    """
    },
    {"patient": 4, "case": "One adjacent visit",
                        "pattern of visits": """
 1111111
        11111111111
"""},
    {
        "patient": 5, "case": "One adjacent visit and a third visit separated by a day",
        "pattern of visits": """
   11111111111
              222222222
                        1111


        """
    },
    {"patient": 5, "case": "Multiple adjacent visits n=4",
     "pattern of visits":
    """

   11
     111111
           11111
                11111
    """
    },
    {"patient": 7, "case": "Overlapping visits",
    "pattern of visits": """
    2222222
        2222222
                                       111111111
    """},
    {"patient": 6, "case": "Overlapping visits with one adjacent",
    "pattern of visits":"""
    2222222
        2222222
               2222
                                11111111
    """},
    {"patient": 6, "case": "Subclaims",
    "pattern of visits":"""
    22222222222222
      2
       22
         2222
                  1111111

                                       33333
    """}

         ]

def parse_pattern_of_visits(pattern_of_visits):

    max_line = 0
    lines_with_data = 0
    for line in pattern_of_visits.split("\n"):
        max_line = max(max_line, len(line))
        if len(line.strip()) > 0:
            lines_with_data += 1


    parsed_pattern_array = np.zeros(shape=(lines_with_data, max_line), dtype="uint8")

    i = 0
    for line in pattern_of_visits.split("\n"):
        if len(line.strip()) > 0:
            j = 0
            for single_character in line:
                if single_character != ' ':
                    parsed_pattern_array[i, j] = int(single_character)
                j += 1
            i += 1

    return parsed_pattern_array

def convert_date_with_add_to_odbc(year, month, day, days_to_add):
    return dt.datetime.strftime(dt.date(year, month, day) + dt.timedelta(days=days_to_add), "%Y-%m-%d")


def generate_cases_as_csv(start_date, cases, patient_dict, hospital_dict, outfile_csv):
    """
    transaction_id,patient_id,patient_name,start_date,end_date,length_of_stay,days_from_beginning,xprovider_id,provider_name
    """
    transaction_id_start = 1000
    transaction_id = transaction_id_start
    start_date_split = start_date.split("-")
    year = int(start_date_split[0])
    month = int(start_date_split[1])
    day = int(start_date_split[2])

    case_list_dict = []
    for case in cases:
        parsed_case = parse_pattern_of_visits(case["pattern of visits"])
        patient = case["patient"]
        patient_name = patient_dict[patient]

        for i in range(parsed_case.shape[0]):
            transaction_id += 1
            state = "before"
            for j in range(parsed_case.shape[1]):
                if parsed_case[i, j] > 0:
                    if state == "before":
                        start_j = j
                        state = "during"
                else:
                    if state == "during":
                        end_j = j - 1
                        provider = parsed_case[i, j-1]
                        provider_name = hospital_dict[provider]
                        start_date = convert_date_with_add_to_odbc(year, month, day, start_j)
                        end_date = convert_date_with_add_to_odbc(year, month, day, end_j)

                        visit = {"start_day": start_j, "end_day": end_j, "patient_id": patient,
                                "provider_id": provider, "provider_name": provider_name,
                                "start_date": start_date, "end_date": end_date, "patient_name": patient_name,
                                "length_of_stay": end_j - start_j, "transaction_id": transaction_id,
                                "visit_type": "inpatient"
                                 }

                        case_list_dict += [visit]
                        state = "before"
            if state == "during":
                end_j = j - 1
                provider = parsed_case[i, j-1]
                provider_name = hospital_dict[provider]
                start_date = convert_date_with_add_to_odbc(year, month, day, start_j)
                end_date = convert_date_with_add_to_odbc(year, month, day, end_j)

                visit = {"start_day": start_j, "end_day": end_j, "patient_id": patient,
                        "provider_id": provider, "provider_name": provider_name,
                        "start_date": start_date, "end_date": end_date, "patient_name": patient_name,
                        "length_of_stay": end_j - start_j, "transaction_id": transaction_id,
                        "visit_type": "inpatient"
                         }

                case_list_dict += [visit]



    pprint.pprint(case_list_dict)

    header = ["transaction_id", "patient_id", "patient_name", "provider_id", "provider_name", "start_day",
              "end_day", "start_date", "end_date", "length_of_stay", "visit_type"]

    with open(outfile_csv, "wb") as fw:
        csv_writer = csv.writer(fw)
        csv_writer.writerow(header)

        for row_dict in case_list_dict:

            row = []
            for field in header:
                row += [row_dict[field]]
            csv_writer.writerow(row)


def print_cases(cases, patient_dict):
    for patient in cases:
        print("Patient: " + patient_dict[patient["patient"]])
        print("Case: " + patient["case"])
        print("*" * 70)
        print(patient["pattern of visits"])
        print("*" * 70)
        print(parse_pattern_of_visits(patient["pattern of visits"]))



if __name__ == "__main__":
    print_cases(CASES, PATIENTS)
    generate_cases_as_csv("2013-01-04", CASES, PATIENTS, HOSPITALS, "inpatient_visits_test.csv")
