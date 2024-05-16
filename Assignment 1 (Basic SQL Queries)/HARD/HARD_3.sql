DROP TABLE IF EXISTS total_stay;
CREATE TEMPORARY TABLE IF NOT EXISTS total_stay as
SELECT 
    i.subject_id,
    i.hadm_id,
    i2.icd_code, i2.icd_version,
    SUM(i.los) as summ
FROM icustays i JOIN procedures_icd i2 ON i.subject_id = i2.subject_id AND i.hadm_id = i2.hadm_id
GROUP BY (i.subject_id, i.hadm_id, i2.icd_code, i2.icd_version)

DROP TABLE IF EXISTS avg_stay;
CREATE TEMPORARY TABLE IF NOT EXISTS avg_stay AS
SELECT
    t.icd_code,
    t.icd_version,
    AVG(t.summ) AS avg_stay
FROM
    total_stay as t
GROUP BY
    t.icd_code,
    t.icd_version

DROP TABLE IF EXISTS relevant_patients;
CREATE TEMPORARY TABLE IF NOT EXISTS relevant_patients AS
SELECT
    p1.subject_id,
    j2.icd_code,
    j2.icd_version,
    p1.gender,
    j1.los
FROM
    patients p1
JOIN
    icustays j1 ON p1.subject_id = j1.subject_id
JOIN
    procedures_icd j2 ON j1.subject_id = j2.subject_id AND j1.hadm_id = j2.hadm_id

SELECT distinct 
    rp.subject_id AS subject_id,
    rp.gender AS gender,
    rp.icd_code AS icd_code,
    rp.icd_version AS icd_version
FROM
    relevant_patients rp
JOIN
    avg_stay a ON rp.icd_code = a.icd_code AND rp.icd_version = a.icd_version
WHERE
    rp.los < a.avg_stay
ORDER BY
    subject_id,
    icd_code DESC,
    icd_version DESC,
    gender
LIMIT 1000

DROP TABLE IF EXISTS total_stay
DROP TABLE IF EXISTS relevant_patients
DROP TABLE IF EXISTS avg_stay