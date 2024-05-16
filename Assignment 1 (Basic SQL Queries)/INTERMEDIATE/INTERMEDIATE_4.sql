select a.subject_id, a.hadm_id, b.icd_code, b.long_title from admissions as a join (select d1.subject_id, d1.hadm_id, d2.icd_code, d2.long_title from diagnoses_icd as d1 join d_icd_diagnoses as d2 on d1.icd_code = d2.icd_code and d1.icd_version = d2.icd_version) as b on a.subject_id = b.subject_id and a.hadm_id = b.hadm_id where a.admission_type = 'URGENT' and a.hospital_expire_flag = 1 and b.long_title is not null order by a.subject_id desc, a.hadm_id desc, b.icd_code desc, b.long_title desc limit 1000