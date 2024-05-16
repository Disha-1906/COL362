select i.subject_id, avg(i.los) as avg_stay_duration 
from (select * from icustays where icustays.los is not null) as i join labevents as l on i.subject_id = l.subject_id and i.hadm_id = l.hadm_id 
where l.itemid = 50878 group by i.subject_id,i.hadm_id order by avg_stay_duration desc, i.subject_id desc limit 1000