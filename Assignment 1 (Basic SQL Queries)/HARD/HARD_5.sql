%%sql with recursive 
earlyadm as (
    select distinct subject_id, hadm_id, admittime, dischtime 
    from admissions order by admittime limit 500
),
graph_edges as (
    select distinct gp.subject_id as node1, a.subject_id as node2
    from earlyadm gp
    join earlyadm a
    on gp.subject_id <> a.subject_id 
    and gp.admittime <= a.dischtime and gp.dischtime >= a.admittime
    join diagnoses_icd d1
    on gp.subject_id = d1.subject_id and gp.hadm_id = d1.hadm_id
    join diagnoses_icd d2
    on a.subject_id = d2.subject_id and a.hadm_id = d2.hadm_id
    and d1.icd_code = d2.icd_code and d1.icd_version = d2.icd_version
), 
PathSearch AS (
SELECT
1 AS length,
subject_id from earlyadm where subject_id = 10001725

UNION ALL

SELECT
ps.length + 1 as length,
earlyadm.subject_id as subject_id
from PathSearch ps join graph_edges 
on graph_edges.node1 = ps.subject_id 
join earlyadm 
on earlyadm.subject_id = graph_edges.node2
where ps.length < 6
)

SELECT
EXISTS (
SELECT 1
FROM
PathSearch
WHERE subject_id =  19438360
AND length <= 6
) AS pathexists;