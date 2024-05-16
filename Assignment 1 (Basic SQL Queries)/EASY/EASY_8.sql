select patients.subject_id, patients.anchor_age from patients join ( select j.subject_id from icustays as j join (select subject_id, hadm_id from diagnoses_icd as d join d_icd_diagnoses as i on d.icd_version = i.icd_version and d.icd_code = i.icd_code where i.long_title like 'Typhoid fever') as g on j.subject_id = g.subject_id and j.hadm_id = g.hadm_id) as f on patients.subject_id = f.subject_id order by patients.subject_id, patients.anchor_age