/* Example of computing readmission time */

/* Step 1 is aggregrate into a single inpatient episode of care */

/* Run this code repeatedly until the number of updated rows equals 0 */

update inpatient_admission_test iat0 set
   union_day_range = t.union_day_range
   from (
   select iat1.transaction_id, iat1.union_day_range + iat2.union_day_range as union_day_range
    from inpatient_admission_test iat1
      join inpatient_admission_test iat2 on
        iat1.patient_id = iat2.patient_id and iat1.transaction_id != iat2.transaction_id and
        iat1.union_day_range && iat2.union_day_range and iat1.union_day_range <> iat2.union_day_range) t
    where iat0.transaction_id = t.transaction_id;
    

 /* This table will hold the ranges that span a single inpatient sstay */
 
drop table if exists inpatient_event_ranges;        
create table inpatient_event_ranges 
  (id serial,
   patient_id integer,
   union_day_range int4range,
   length_of_stay_in_days int);

insert into inpatient_event_ranges (patient_id, union_day_range,  length_of_stay_in_days)
  select patient_id, union_day_range, upper(union_day_range) - lower(union_day_range) - 1
   from inpatient_admission_test group by patient_id, union_day_range
    order by patient_id;

/* Create a table which pairs an inpatient stay with the past inpatient stay */
drop table if exists inpatient_paired_past_discharges;
create table inpatient_paired_past_discharges as 
  select lower(ier1.union_day_range) - upper(ier2.union_day_range) as days_since_paired_discharge, 
    ier1.patient_id, ier1.union_day_range as target_union_date, ier2.union_day_range as previous_target_union_date, 
    ier1.id as target_id, ier2.id as previous_id
    from inpatient_event_ranges ier1 
      join inpatient_event_ranges ier2 on ier1.patient_id = ier2.patient_id and
        ier2.union_day_range << ier1.union_day_range;
        
        
/* Here we connect an admission to the last admission */
drop table if exists inpatient_events_with_readmission;
create table inpatient_events_with_readmission as 
  select ier.id, ier.patient_id, ier.union_day_range, ier.length_of_stay_in_days,
    tt.id_last_admission, tt.days_since_last_discharge
    from inpatient_event_ranges ier left outer join (
      select distinct ippd.target_id as id_current_admission, previous_id as id_last_admission, 
        t.min_days_since_last_discharge + 1 as days_since_last_discharge
        from inpatient_paired_past_discharges ippd join 
        (        
          select target_id, patient_id, min(days_since_paired_discharge) as min_days_since_last_discharge 
            from inpatient_paired_past_discharges group by target_id, patient_id    
        ) t 
          on t.target_id = ippd.target_id and 
          t.min_days_since_last_discharge  = ippd.days_since_paired_discharge) tt 
          on ier.id = tt.id_current_admission;

/* Add in forward chains */
alter table inpatient_events_with_readmission add id_next_admission int;

update inpatient_events_with_readmission ier set id_next_admission = ierb.id 
  from inpatient_events_with_readmission ierb
    where ier.id = ierb.id_last_admission;
    

/* View the table inpatient_events */    
select * from inpatient_events_with_readmission order by patient_id, id;

/*
id	patient_id	union_day_range	length_of_stay_in_days	id_last_admission	days_since_last_discharge	id_next_admission
1	1	[1,3)	1			2
2	1	[9,16)	6	1	7	3
3	1	[26,32)	5	2	11
4	3	[55,62)	6
5	4	[1,8)	6			6
6	4	[8,18)	9	5	1
7	5	[3,23)	19			8
8	5	[24,27)	2	7	2
9	6	[4,15)	10			10
10	6	[15,19)	3	9	1	11
11	6	[32,39)	6	10	14
12	7	[4,15)	10			13
13	7	[39,47)	7	12	25
14	8	[4,18)	13			15
15	8	[18,25)	6	14	1	16
16	8	[39,43)	3	15	15

 */
