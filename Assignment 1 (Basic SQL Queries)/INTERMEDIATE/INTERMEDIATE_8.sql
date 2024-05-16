select d.subject_id, d.hadm_id, count(distinct d.icd_code) as distinct_diagnoses_count, p.drug  
from diagnoses_icd as d join prescriptions as p on d.subject_id = p.subject_id and d.hadm_id = p.hadm_id 
where d.icd_code like 'V4%' and (p.drug ilike '%prochlorperazine%' or p.drug ilike '%bupropion%') 
group by (d.subject_id,d.hadm_id,p.drug) having count(distinct d.icd_code)>1 
order by distinct_diagnoses_count desc, d.subject_id desc, d.hadm_id desc, p.drug