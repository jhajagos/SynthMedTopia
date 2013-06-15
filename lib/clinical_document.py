__author__ = 'janos'

"""
Need separate collections for providers and patients. The last collection that is built is the provider encounter
collection.
"""

import pprint

clinical_encounter_template = {"encounter_number": None, "generated_identifier": None,
                      "patient": {"medical_record_number": None,
                                   "generated_identifier": None,
                                   "gender": None,
                                   "date_of_birth": None,
                                   "year_of_birth": None,
                                   "age_at_time_of_encounter": None},
                      "providers": {
                          "attending_provider": {
                            "npi":   None,
                            "generated_identifier": None,
                            "first_name": None,
                            "last_name": None,
                            "local_identifier": None,
                          },
                        "servicing_provider": {
                            "npi":   None,
                            "generated_identifier": None,
                            "first_name": None,
                            "last_name": None,
                            "local_identifier": None
                        },
                        "billing_provider": {
                            "npi":   None,
                            "generated_identifier": None,
                            "first_name": None,
                            "last_name": None,
                            "local_identifier": None
                        }},
                        "start_date_time": None,
                        "end_date_time": None,
                        "vital_signs":
                            {"blood_pressure": {
                                "date_time_recorded":  None,
                                "systolic": None,
                                "diastolic": None,
                                "measurement_unit": None,
                                "measurement_full": None

                            },
                            "body_mass": {
                                "date_time_recorded": None,
                                "mass": None
                            },
                            "pulse": {
                                "date_time_recorded": None,
                                "measurement": None,
                                "measurement_unit": None,
                                "measurement_full": None
                            },
                            "mass": {
                                "date_time_recorded": None,
                                "measurement": None,
                                "measurement_unit": None,
                                "measurement_full": None
                            }
                        },
                        "diagnoses_made":
                        [{"code": None, "description": None, "code_type": None }],
                        "dx_codes": [],
                        "procedures_performed": [{"code": None, "description": None, "code_type": None,"date_time_recorded": None}],
                        "procedure_codes": [],
                        "procedure_codes_with_associated_dx" : [{}],
                        "drugs_prescribed": [{"code": None, "description": None, "code_type": None, "prescribing_method": None, "date_time_recorded": None}] # Active medications. These are in the clinical notes?
    }


def patient_encounter_dict_generate(medical_record_number=None, generated_identifier=None, gender=None, date_of_birth=None, year_of_birth = None, age_at_time_of_encounter = None):
    patient_dict = {"medical_record_number": medical_record_number,
                    "generated_identifier": generated_identifier,
                    "gender": gender,
                    "date_of_birth": date_of_birth,
                    "year_of_birth": year_of_birth,
                    "age_at_time_of_encounter": age_at_time_of_encounter}
    return patient_dict


def provider_dict_generate(npi=None, generated_identifier=None, first_name=None, last_name=None, local_identifier=None):
    provider_dict = {"npi":   npi,
        "generated_identifier": generated_identifier,
        "first_name": first_name,
        "last_name": last_name,
        "local_identifier": local_identifier}
    return provider_dict


def measurement_dict_generate(date_time_recorded=None, measurement=None, measurement_full=None, measurement_unit=None):
    return {'date_time_recorded': date_time_recorded,
                          'measurement': measurement,
                          'measurement_full': measurement_full,
                          'measurement_unit': measurement_unit}


pprint.pprint(clinical_encounter_template)

