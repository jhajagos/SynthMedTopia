__author__ = 'Janos Hajagos'

import numpy as np
import json

"""
    Here is the detail of the proposed medical data sets.

    Artificial data sets:

    General:

    All code will be written in Python. The only dependency will be that the Numpy package is installed.  Model parameters will be configured with JSON files. Numbers will be rounded to normal recorded medical accuracy.  The general goal is to make the data have the "smell" of being real with real outliers.  Generating programs will be implemented in order.

    Set 1: Extract from Electronic Health Record

    Variables:
        PMRN (Pseudo Medical Record Number)
        Observation date (mm/dd/yyyy hh:mm)
        Gender (M,F,N)
        Date of birth (mm/dd/yyyy)
        Height (meters)
        Body mass (kg)
        Pulse (beats per minute)
        Systolic blood pressure (mm Hg)
        Diastolic blood pressure (mm Hg)

    Target Size < 10,000 observations

    Modelling strategy:
        Minimum age of 20 years
        Maximum age of 65 years
        Gender (M,F,N)
        Females and males will be modeled separately
        Height will be based on censored normal distribution
        BMI (Body Mass Index) will be based on a bimodal distribution (a mixture of censored normals)
        Mass will be determined from the BMI
        Difference between systolic and diastolic will be based on a tail heavy distribution
        Midpoint between systolic and diastolic will be censored bimodal
        Pulse will have a linear trend (decreasing with age)

    Potential analyses:
        Descriptive statistics
        Descriptive statistics by age band
        Correlation analysis

    Set 2: Administrative diagnosis and procedure codes
        Pseudo Member Id
        Gender (M,F,N)
        Diagnosis code 1 (ICD9CM)
        Diagnosis code 2 (ICD9CM)
        Diagnosis code 3 (ICD9CM)
        Diagnosis code 4 (ICD9CM)
        Date of birth (mm/dd/yyyy)
        Date of service (mm/dd/yyyy)
        Procedure performed (ICD9CM)
        Zip code of member
        PPI (Pseudo Provider Identifier)

    Target Size < 100,000

    Modelling strategy:
        Minimum age of 20 years
        Maximum age of 45 years
        Separate age distribution for females (heavier at younger age)
        Distance from city center will exponentially decline
        Direction will be selected by a uniform(0,2 Pi)
        Female diagnoses will be selected based on public sources
        Male diagnoses will be selected based on public sources
        Protected diagnoses will be included
        Some procedures will not be diagnosis specific
        Other procedures will be diagnosis specific

    Potential analyses:
        Identifying cohorts for clinical studies
        Geographic analysis

    Set 3: Shared patient provider network

    Node Variables:
        PPI (Pseudo Provider Identifier)
        is_physician
        is_transport
        is_radiology
        is_specialist
        is_pathology
        zip code of provider

    Edge variables
        PPI_from
        PPI_to

    Target Size < 1,000,000 links to 20,000 providers

    Modeling strategy:
        Proportions of provider types will be estimated from the public NPPES data set.
        A random graph model such as binomial.
        Weights will be estimated based on provider types (log normal distribution)

    Potential analyses:
      Network based analyses
           Distribution of in and out degrees
           Effect on censoring the data

"""


def read_configuration(config_file_name):
    f = open(config_file_name)
    configuration = json.load(f)
    f.close()
    return configuration


def main():
    pass

if __name__ == "__main__":
    main()


