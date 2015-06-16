__author__ = 'janos'

"""
    Load
"""

import sqlalchemy as sa
import config_db
import csv
import re

def main():
    create_table_sql = """
    drop table if exists synthetic_diagnosis_data;
    create table synthetic_diagnosis_data
      (
           patient_id INTEGER,
           encounter_id INTEGER,
           sequence_id INTEGER,
           encounter_day INTEGER,
           ccs_diagnosis VARCHAR(15),
           ccs_diagnosis_description VARCHAR(255),
           gender VARCHAR(5),
           age INTEGER,
           date_of_encounter_text VARCHAR(255),
           date_of_encounter DATE
       );
    """

    engine = sa.create_engine(config_db.connection_url)
    print(config_db.connection_url)
    cursor = engine.connect()
    for statement in create_table_sql.split(";"):
        if len(statement.strip()):
            print(statement.strip() + ";")
            print("")
            cursor.execute(statement)

    re_integer = re.compile("^[0-9]+$")

    metadata = sa.MetaData(engine, reflect=True)
    diagnosis_table = metadata.tables["synthetic_diagnosis_data"]
    with open("./synthetic_diagnosis_data_set.csv", "rb") as f:
        csv_dict_reader = csv.DictReader(f)


        for row in csv_dict_reader:
            for key in row:
                if re_integer.match(row[key]) is not None:
                    row[key] = int(row[key])


            insert_ddl = diagnosis_table.insert(row)
            cursor.execute(insert_ddl)

    print("Updating dates of visits")
    cursor.execute("update synthetic_diagnosis_data set date_of_encounter = cast(date_of_encounter_text as date)")


    print("Building indices")
    cursor.execute("create index idx_sdd_pi on synthetic_diagnosis_data(patient_id)")
    cursor.execute("create index idx_sdd_ec on synthetic_diagnosis_data(encounter_id)")
    cursor.execute("create index idx_sdd_eday on synthetic_diagnosis_data(encounter_day)")
    cursor.execute("create index idx_sdd_ccs on synthetic_diagnosis_data(ccs_diagnosis)")



if __name__ == "__main__":
    main()