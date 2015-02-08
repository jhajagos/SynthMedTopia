__author__ = 'janos'

"""
    Load "inpatient_visits_test.csv" into a PostGreSQL database.
"""

import sqlalchemy as sa
import config_db
import csv


def main():

    create_table_sql = """
    drop table if exists inpatient_admission_test;
    create table inpatient_admission_test
      (transaction_id INTEGER,
       patient_id INTEGER,
       patient_name VARCHAR(255),
       provider_id integer,
       provider_name VARCHAR(255),
       start_day INTEGER,
       start_date date,
       end_day INTEGER,
       end_date date,
       length_of_stay INTEGER,
       visit_type VARCHAR(15),
       day_range int4range,
       date_range daterange)
    """

    engine = sa.create_engine(config_db.connection_url)
    print(config_db.connection_url)
    cursor = engine.connect()
    for statement in create_table_sql.split(";"):
        if len(statement.strip()):
            print(statement.strip() + ";")
            print("")
            cursor.execute(statement)

    with open("./inpatient_visits_test.csv", "rb") as f:
        csv_dict_reader = csv.DictReader(f)
        for row in csv_dict_reader:
            cols = row.keys()
            field_list_string = ""
            for field in cols:
                field_list_string += field + ", "

            field_list_string = field_list_string[:-2]

            insert_value_list = []
            for field in cols:

                try:
                    value = int(row[field])
                except ValueError:
                    value = row[field]

                insert_value_list += [value]
            values_parens = str(tuple(insert_value_list))

            sql_string = "insert into inpatient_admission_test\n (%s)\n values %s" % (field_list_string, values_parens)
            print(sql_string + ";")

            cursor.execute(sql_string)

        sql_string = "update inpatient_admission_test set day_range = int4range(start_day, end_day + 1, '[)')"
        print(sql_string)
        cursor.execute(sql_string)

        sql_string = "update inpatient_admission_test set date_range = daterange(start_date, end_date + 1, '[)')"
        cursor.execute(sql_string)

if __name__ == "__main__":
    main()