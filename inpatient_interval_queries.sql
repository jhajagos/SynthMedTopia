alter table inpatient_admission_test add union_day_range int4range;

update inpatient_admission_test set union_day_range = day_range;

update inpatient_admission_test iat0 set
   union_day_range = t.union_day_range
   from (
   select iat1.transaction_id, iat1.union_day_range + iat2.union_day_range as union_day_range
    from inpatient_admission_test iat1
      join inpatient_admission_test iat2 on
        iat1.patient_id = iat2.patient_id and iat1.transaction_id != iat2.transaction_id and
        iat1.union_day_range && iat2.union_day_range and iat1.union_day_range <> iat2.union_day_range) t
    where iat0.transaction_id = t.transaction_id;

select transaction_id from inpatient_admission_test  except
  select * from
      (select transaction_id1 from inpatient_joined1
        union select transaction_id2 from inpatient_joined1) t;


/*
select iat1.patient_id, iat1.transaction_id as transaction_id1,
    iat2.transaction_id as transaction_id2,
      iat1.day_range as day_range1, iat2.day_range as day_range2,
      iat1.day_range + iat2.day_range as combined_day_range
    from inpatient_admission_test iat1
    join inpatient_admission_test iat2 on
        iat1.patient_id = iat2.patient_id and iat1.transaction_id != iat2.transaction_id and
        iat1.day_range && iat2.day_range;

 patient_id | transaction_id1 | transaction_id2 | day_range1 | day_range2 | combined_day_range
------------+-----------------+-----------------+------------+------------+--------------------
          5 |            1007 |            1012 | [3,14)     | [11,16)    | [3,16)
          5 |            1007 |            1011 | [3,14)     | [5,11)     | [3,14)
          5 |            1007 |            1010 | [3,14)     | [3,5)      | [3,14)
          5 |            1008 |            1012 | [14,23)    | [11,16)    | [11,23)
          5 |            1010 |            1007 | [3,5)      | [3,14)     | [3,14)
          5 |            1011 |            1007 | [5,11)     | [3,14)     | [3,14)
          5 |            1012 |            1008 | [11,16)    | [14,23)    | [11,23)
          5 |            1012 |            1007 | [11,16)    | [3,14)     | [3,16)
          7 |            1014 |            1015 | [4,11)     | [8,15)     | [4,15)
          7 |            1015 |            1014 | [8,15)     | [4,11)     | [4,15)
          6 |            1017 |            1024 | [4,11)     | [9,13)     | [4,13)
          6 |            1017 |            1023 | [4,11)     | [7,9)      | [4,11)
          6 |            1017 |            1022 | [4,11)     | [6,7)      | [4,11)
          6 |            1017 |            1021 | [4,11)     | [4,18)     | [4,18)
          6 |            1017 |            1018 | [4,11)     | [8,15)     | [4,15)
          6 |            1018 |            1024 | [8,15)     | [9,13)     | [8,15)
          6 |            1018 |            1023 | [8,15)     | [7,9)      | [7,15)
          6 |            1018 |            1021 | [8,15)     | [4,18)     | [4,18)
          6 |            1018 |            1017 | [8,15)     | [4,11)     | [4,15)
          6 |            1019 |            1025 | [15,19)    | [18,25)    | [15,25)
          6 |            1019 |            1021 | [15,19)    | [4,18)     | [4,19)
          6 |            1021 |            1024 | [4,18)     | [9,13)     | [4,18)
          6 |            1021 |            1023 | [4,18)     | [7,9)      | [4,18)
          6 |            1021 |            1022 | [4,18)     | [6,7)      | [4,18)
          6 |            1021 |            1019 | [4,18)     | [15,19)    | [4,19)
          6 |            1021 |            1018 | [4,18)     | [8,15)     | [4,18)
          6 |            1021 |            1017 | [4,18)     | [4,11)     | [4,18)
          6 |            1022 |            1021 | [6,7)      | [4,18)     | [4,18)
          6 |            1022 |            1017 | [6,7)      | [4,11)     | [4,11)
          6 |            1023 |            1021 | [7,9)      | [4,18)     | [4,18)
          6 |            1023 |            1018 | [7,9)      | [8,15)     | [7,15)
          6 |            1023 |            1017 | [7,9)      | [4,11)     | [4,11)
          6 |            1024 |            1021 | [9,13)     | [4,18)     | [4,18)
          6 |            1024 |            1018 | [9,13)     | [8,15)     | [8,15)
          6 |            1024 |            1017 | [9,13)     | [4,11)     | [4,13)
          6 |            1025 |            1019 | [18,25)    | [15,19)    | [15,25)
 */