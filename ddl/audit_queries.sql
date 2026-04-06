--assessing unmapped ids not matched between workforce_map and employees/contractors/learners/volunteers
SELECT
  id
FROM employees
WHERE termination_date IS NULL OR
      YEAR(termination_date) >= 2023
EXCEPT
SELECT
  employee_id
FROM workforce_map;

SELECT
  employee_id
FROM workforce_map
WHERE employee_id IS NOT NULL
EXCEPT
SELECT
  id
FROM employees;

SELECT
  id
FROM contractors
EXCEPT
SELECT
  contractor_id
FROM workforce_map;

SELECT
  contractor_id
FROM workforce_map
WHERE contractor_id IS NOT NULL
EXCEPT
SELECT
  id
FROM contractors;

SELECT
  id
FROM learners
EXCEPT
SELECT
  learner_id
FROM workforce_map;

SELECT
  learner_id
FROM workforce_map
WHERE learner_id IS NOT NULL
EXCEPT
SELECT
  id
FROM learners;

SELECT
  id
FROM volunteers
EXCEPT
SELECT
  volunteer_id
FROM workforce_map;

SELECT
  volunteer_id
FROM workforce_map
WHERE volunteer_id IS NOT NULL
EXCEPT
SELECT
  id
FROM volunteers;

SELECT DISTINCT
  CAST(CLARITY_SER.user_id AS varchar(18)) AS user_id
FROM Clarity_SA'EPIC ID'.dbo.CLARITY_SER
  INNER JOIN Clarity_SA'EPIC ID'.dbo.PAT_ENC ON CLARITY_SER.PROV_ID = PAT_ENC.VISIT_PROV_ID
WHERE CLARITY_SER.SERV_AREA_ID = 'EPIC ID' AND
      CLARITY_SER.user_id IS NOT NULL AND
      CLARITY_SER.user_id != 'EPIC ID'09999
EXCEPT
SELECT
  CAST(epic_id AS varchar(18))
FROM workforce_map
WHERE epic_id IS NOT NULL;

SELECT
  CAST(epic_id AS varchar(18)) AS epic_id
FROM workforce_map
WHERE epic_id IS NOT NULL
EXCEPT
SELECT DISTINCT
  CAST(CLARITY_EMP.user_id AS varchar(18))
FROM Clarity_SA'EPIC ID'.dbo.CLARITY_EMP;

SELECT
  id,
  source,
  first_name,
  last_name,
  common_name,
  network_email,
  primary_email,
  email_alias,
  job_title,
  account_name,
  active_dir_group,
  department,
  location,
  user_description,
  phone_extension,
  mobile_phone,
  created_at,
  inserted_at,
  is_disabled,
  disabled_at,
  is_deleted,
  deleted_at
FROM network_users
WHERE id NOT IN (
  SELECT network_id
  FROM workforce_map
) AND
      is_primary_acct = 1 AND
      is_non_user = 0 AND
      COALESCE(YEAR(disabled_at), 2024) >= 2024 AND
      COALESCE(YEAR(deleted_at), 2024) >= 2024;

--Checking for duplicate ids within workforce_map
SELECT
  network_id,
  COUNT(*) AS count
FROM workforce_map
GROUP BY network_id
HAVING COUNT(*) > 1;

SELECT
  employee_id,
  COUNT(*) AS count
FROM workforce_map
WHERE employee_id IS NOT NULL
GROUP BY employee_id
HAVING COUNT(*) > 1;

SELECT
  contractor_id,
  COUNT(*) AS count
FROM workforce_map
WHERE contractor_id IS NOT NULL
GROUP BY contractor_id
HAVING COUNT(*) > 1;

SELECT
  learner_id,
  COUNT(*) AS count
FROM workforce_map
WHERE learner_id IS NOT NULL
GROUP BY learner_id
HAVING COUNT(*) > 1;

SELECT
  volunteer_id,
  COUNT(*) AS count
FROM workforce_map
WHERE volunteer_id IS NOT NULL
GROUP BY volunteer_id
HAVING COUNT(*) > 1;

SELECT
  relevant_id,
  COUNT(*) AS count
FROM workforce_map
WHERE relevant_id IS NOT NULL
GROUP BY relevant_id
HAVING COUNT(*) > 1;

SELECT
  epic_id,
  COUNT(*) AS count
FROM workforce_map
WHERE epic_id IS NOT NULL
GROUP BY epic_id
HAVING COUNT(*) > 1;

WITH all_ecw_ids AS
       (
         SELECT
           id,
           'ecw instance 1' AS source,
           value AS ecw_id
         FROM workforce_map
           CROSS APPLY OPENJSON(ecw_ids, '$.ecw instance 1')
         UNION
         SELECT
           id,
           'ecw instance 2' AS source,
           value AS ecw_id
         FROM workforce_map
           CROSS APPLY OPENJSON(ecw_ids, '$.ecw instance 2')
       )
SELECT
  source,
  ecw_id,
  COUNT(*) AS count
FROM all_ecw_ids
GROUP BY source, ecw_id
HAVING COUNT(*) > 1;

-- (rest unchanged until ECW joins)

WITH ecw1_ids AS
       (
         SELECT
           id,
           network_id,
           value AS ecw_id
         FROM workforce_map
           CROSS APPLY OPENJSON(ecw_ids, '$.ecw instance 1')
       ),
     ecw2_ids AS
       (
         SELECT
           id,
           network_id,
           value AS ecw_id
         FROM workforce_map
           CROSS APPLY OPENJSON(ecw_ids, '$.ecw instance 2')
       )
SELECT
  ecw1_ids.id,
  'ecw instance 1' AS source,
  ecw1_users.uid AS ecw_id,
  COALESCE(email_alias, primary_email, network_email) AS network_email,
  LOWER(TRIM(ecw1_users.uemail)) AS ecw_email,
  IIF(common_name LIKE '% %' AND PATINDEX('%[0-9]%', common_name) = 0, network_users.common_name,
      CONCAT(network_users.first_name, ' ', network_users.last_name)) AS network_name,
  CONCAT_WS(' ', ecw1_users.ufname,
            ecw1_users.ulname) AS ecw_name
FROM ecw1_ids
  LEFT JOIN network_users ON ecw1_ids.network_id = network_users.id
  LEFT JOIN ecw1_users ON ecw1_ids.ecw_id = ecw1_users.uid
WHERE usertype IN (1, 2, 9)
UNION
SELECT
  ecw2_ids.id,
  'ecw instance 2' AS source,
  ecw2_users.uid AS ecw_id,
  COALESCE(email_alias, primary_email, network_email) AS network_email,
  LOWER(TRIM(ecw2_users.uemail)) AS ecw_email,
  IIF(common_name LIKE '% %' AND PATINDEX('%[0-9]%', common_name) = 0, network_users.common_name,
      CONCAT(network_users.first_name, ' ', network_users.last_name)) AS network_name,
  CONCAT_WS(' ', ecw2_users.ufname,
            ecw2_users.ulname) AS ecw_name
FROM ecw2_ids
  LEFT JOIN network_users ON ecw2_ids.network_id = network_users.id
  LEFT JOIN ecw2_users ON ecw2_ids.ecw_id = ecw2_users.uid
WHERE usertype IN (1, 2, 9);
