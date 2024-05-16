With 
Num as (
select gender as g, count(subject_id) as con from patients as p 
where p.subject_id in (select d1.subject_id from diagnoses_icd as d1 join d_icd_diagnoses as d2 on d1.icd_code = d2.icd_code and d1.icd_version = d2.icd_version where d2.long_title like '%Meningitis%'
and exists (select 1 from admissions as a 
where a.subject_id = d1.subject_id and a.hadm_id = d1.hadm_id and a.admittime  = ( select max(admittime) from admissions where subject_id = a.subject_id))) group by gender)
, PAK as (select gender, count(subject_id) as c1 from patients as p where p.subject_id in (select d1.subject_id from diagnoses_icd as d1 join d_icd_diagnoses as d2 on d1.icd_code = d2.icd_code and d1.icd_version = d2.icd_version where d2.long_title like '%Meningitis%' and exists (select 1 from admissions as a where a.subject_id = d1.subject_id and a.hadm_id = d1.hadm_id and a.admittime  = ( select max(admittime) from admissions where subject_id = a.subject_id) and hospital_expire_flag = 1)) group by gender)
select Num.g as gender, round(PAK.c1 * 100.0 / Num.con,2) as mortality_rate from Num join PAK on Num.g = PAK.gender order by mortality_rate, gender desc