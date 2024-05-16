select distinct a1.subject_id from admissions as a1 join admissions as a2 on a1.subject_id =a2.subject_id and a1.hadm_id<>a2.hadm_id
where (a1.subject_id,a1.hadm_id) in (select subject_id, hadm_id from diagnoses_icd as d where d.icd_code like 'I21%') and a1.dischtime<a2.admittime
order by a1.subject_id desc limit 1000