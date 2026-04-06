DROP VIEW IF EXISTS view_workforce;
CREATE VIEW view_workforce AS
WITH workforce_hours AS
       (
         SELECT
           workforce_map.id,
           'Employee' AS source,
           SUM(paid_minutes) / 60.0 AS hours
         FROM workforce.dbo.workforce_map
           INNER JOIN workforce.dbo.employees ON workforce_map.employee_id = employees.id
           INNER JOIN workforce.dbo.employee_hours ON employees.id = employee_hours.employee_id
           INNER JOIN workforce.dbo.employee_job_titles ON employees.job_title_id = employee_job_titles.id
         WHERE YEAR(date) = 2025
         GROUP BY workforce_map.id
         UNION
         SELECT
           workforce_map.id,
           'Contractor' AS source,
           CASE
             WHEN calculated_hours.contractor_id = 24
               THEN calculated_hours.hours / 5.0
             WHEN calculated_hours.contractor_id = 23
               THEN calculated_hours.hours / 4.0
             ELSE
               calculated_hours.hours
             END AS hours
         FROM workforce.dbo.workforce_map
           INNER JOIN (
           SELECT
             contractor_id,
             SUM(hours) AS hours
           FROM workforce.dbo.contractor_job_hours
           WHERE YEAR(contractor_job_hours.start_date) = 2025
           GROUP BY contractor_id
         ) calculated_hours
                      ON workforce_map.contractor_id = calculated_hours.contractor_id
         UNION
         SELECT
           workforce_map.id,
           'Learner' AS source,
           ROUND((CAST(
                    DATEDIFF(DAY,
                             IIF(start_date < '2025-01-01', '2025-01-01', start_date),
                             IIF(end_date > '2025-12-31', '2025-12-31', end_date)
                    ) + 1 AS FLOAT
                  ) / CAST(DATEDIFF(DAY, start_date, end_date) + 1 AS FLOAT)) * learner_job_hours.hours,
                 2) AS hours
         FROM workforce.dbo.workforce_map
           INNER JOIN workforce.dbo.learner_job_hours ON workforce_map.learner_id = learner_job_hours.learner_id
         WHERE end_date >= '2025-01-01' AND
               start_date <= '2025-12-31' AND
               learner_job_hours.hours > 0
         UNION
         SELECT
           workforce_map.id,
           'Volunteer' AS source,
           volunteer_job_hours.hours
         FROM workforce.dbo.workforce_map
           INNER JOIN workforce.dbo.volunteer_job_hours ON workforce_map.volunteer_id = volunteer_job_hours.volunteer_id
         WHERE end_date >= '2025-01-01' AND
               start_date <= '2025-12-31' AND
               volunteer_job_hours.hours > 0
       ),
     calendar_year_ftes AS
       (
         SELECT
           workforce_map.id,
           STRING_AGG(source, ' | ') AS calendar_year_hours_source,
           ROUND(SUM(workforce_hours.hours), 2) AS calendar_year_hours,
           ROUND(SUM(workforce_hours.hours) / 2080, 2) AS calendar_year_ftes
         FROM workforce.dbo.workforce_map
           INNER JOIN workforce_hours ON workforce_map.id = workforce_hours.id
         GROUP BY workforce_map.id
       )
SELECT
  workforce_map.id,
  workforce_map.network_id,
  workforce_map.employee_id,
  workforce_map.contractor_id,
  workforce_map.learner_id,
  workforce_map.volunteer_id,
  COALESCE(calendar_year_ftes.calendar_year_hours_source,
           CASE
             WHEN workforce_map.employee_id IS NOT NULL THEN 'Employee'
             WHEN workforce_map.contractor_id IS NOT NULL THEN 'Contractor'
             WHEN workforce_map.learner_id IS NOT NULL THEN 'Learner'
             WHEN workforce_map.volunteer_id IS NOT NULL THEN 'Volunteer' END) AS staff_type,
  CASE
    WHEN workforce_map.employee_id IS NOT NULL THEN CONCAT(employees.legal_first_name, ' ', employees.legal_last_name)
    WHEN network_users.common_name LIKE '% %' AND PATINDEX('%[0-9]%', common_name) = 0 THEN common_name
    WHEN workforce_map.contractor_id IS NOT NULL THEN CONCAT(contractors.first_name, ' ', contractors.last_name)
    WHEN workforce_map.learner_id IS NOT NULL THEN CONCAT(learners.first_name, ' ', learners.last_name)
    WHEN workforce_map.volunteer_id IS NOT NULL
      THEN CONCAT(volunteers.first_name, ' ', volunteers.last_name) END AS staff_name,
  COALESCE(network_users.primary_email, network_users.email, network_users.network_email, employees.work_email,
           contractors.email, learners.email, volunteers.email) AS email,
  COALESCE(employee_job_titles.job_title, contractors.job_title, learners.job_title, volunteers.job_title,
           network_users.job_title) AS job_title,
  COALESCE(employee_job_titles.uds_line, contractors.uds_line, learners.uds_line,
           volunteers.uds_line) AS current_uds_line,
  COALESCE(employee_job_titles.hcai_line, contractors.hcai_line, learners.hcai_line,
           volunteers.hcai_line) AS current_hcai_line,
  CASE
    WHEN employees.status = 'Terminated' OR network_users.is_deleted = 1 THEN 'Terminated'
    WHEN employees.status = 'Inactive' OR network_users.is_disabled = 1 THEN 'Inactive'
    ELSE 'Active' END AS status,
  FORMAT(COALESCE(employees.hire_date, employees.start_date, network_users.created_at, contractor_job_hours.start_date,
                  learner_job_hours.start_date,
                  volunteer_job_hours.start_date), 'yyyy-MM-dd') AS start_date,
  FORMAT(COALESCE(employees.termination_date, network_users.disabled_at, network_users.deleted_at),
         'yyyy-MM-dd') AS end_date,
  epic_id AS epic_user_id,
  clarity_ser.prov_id AS epic_prov_id,
  ecw_ids,
  calendar_year_ftes.calendar_year_hours,
  calendar_year_ftes.calendar_year_ftes
FROM workforce.dbo.workforce_map
  LEFT JOIN workforce.dbo.network_users
            ON workforce_map.network_id = network_users.id
  LEFT JOIN calendar_year_ftes ON workforce_map.id = calendar_year_ftes.id
  LEFT JOIN workforce.dbo.employees ON workforce_map.employee_id = employees.id
  LEFT JOIN workforce.dbo.departments AS employee_dep ON employees.department_id = employee_dep.id
  LEFT JOIN workforce.dbo.locations AS employee_loc ON employees.location_id = employee_loc.id
  LEFT JOIN workforce.dbo.employee_job_titles ON employees.job_title_id = employee_job_titles.id
  LEFT JOIN workforce.dbo.contractors ON workforce_map.contractor_id = contractors.id
  LEFT JOIN (
  SELECT
    contractor_id,
    job_title,
    MIN(start_date) AS start_date,
    MAX(end_date) AS end_date
  FROM workforce.dbo.contractor_job_hours
  GROUP BY contractor_id,
           job_title
) AS contractor_job_hours ON contractors.id = contractor_job_hours.contractor_id AND
                             contractors.job_title = contractor_job_hours.job_title
  LEFT JOIN workforce.dbo.learners ON workforce_map.learner_id = learners.id
  LEFT JOIN workforce.dbo.learner_job_hours
            ON learners.id = learner_job_hours.learner_id AND learners.job_title = learner_job_hours.job_title
  LEFT JOIN workforce.dbo.volunteers ON workforce_map.volunteer_id = volunteers.id
  LEFT JOIN workforce.dbo.volunteer_job_hours
            ON volunteers.id = volunteer_job_hours.volunteer_id AND volunteers.job_title = volunteer_job_hours.job_title
  LEFT JOIN Clarity_SA279.dbo.clarity_ser ON CAST(epic_id AS VARCHAR(18)) = clarity_ser.user_id;

DROP VIEW IF EXISTS view_workforce_hours;
CREATE VIEW view_workforce_hours AS
WITH current_fte_dates AS
       (
         SELECT
           date
         FROM workforce.dbo.employee_hours
         WHERE YEAR(date) = 2025
         GROUP BY date
         HAVING COUNT(date) > 600 AND
                date <= GETDATE()
       ),
     current_fte_hours AS
       (
         SELECT
           COUNT(*) * 8 AS count
         FROM current_fte_dates
       ),
     total_workforce_hours AS
       (
         SELECT
           workforce_map.id,
           'Employee' AS source,
           employee_job_titles.job_title,
           employee_job_titles.uds_line,
           employee_job_titles.hcai_line,
           MIN(date) AS start_date,
           MAX(date) AS end_date,
           SUM(paid_minutes) / 60.0 AS hours
         FROM workforce.dbo.workforce_map
           INNER JOIN workforce.dbo.employee_hours ON workforce_map.employee_id = employee_hours.employee_id
           INNER JOIN workforce.dbo.employee_job_titles
                      ON employee_hours.job_title_id = employee_job_titles.id
         WHERE YEAR(date) = 2025
         GROUP BY workforce_map.id,
                  employee_job_titles.job_title,
                  employee_job_titles.uds_line,
                  employee_job_titles.hcai_line
         UNION
         SELECT
           workforce_map.id,
           'Contractor' AS source,
           calculated_hours.job_title,
           calculated_hours.uds_line,
           calculated_hours.hcai_line,
           calculated_hours.start_date,
           calculated_hours.end_date,
           CASE
             WHEN calculated_hours.contractor_id = 24
               THEN calculated_hours.hours / 5.0
             WHEN calculated_hours.contractor_id = 23
               THEN calculated_hours.hours / 4.0
             ELSE
               calculated_hours.hours
             END AS hours
         FROM workforce.dbo.workforce_map
           INNER JOIN (
           SELECT
             contractor_id,
             job_title,
             uds_line,
             hcai_line,
             MIN(start_date) AS start_date,
             MAX(end_date) AS end_date,
             SUM(hours) AS hours
           FROM workforce.dbo.contractor_job_hours
           WHERE YEAR(contractor_job_hours.start_date) = 2025
           GROUP BY contractor_id, job_title, uds_line, hcai_line
         ) calculated_hours
                      ON workforce_map.contractor_id = calculated_hours.contractor_id
         UNION
         SELECT
           workforce_map.id,
           'Learner' AS source,
           learner_job_hours.job_title,
           learner_job_hours.uds_line,
           learner_job_hours.hcai_line,
           IIF(start_date < '2025-01-01', '2025-01-01', start_date) AS start_date,
           IIF(end_date > '2025-12-31', '2025-12-31', end_date) AS end_date,
           ROUND((CAST(
                    DATEDIFF(DAY,
                             IIF(start_date < '2025-01-01', '2025-01-01', start_date),
                             IIF(end_date > '2025-12-31', '2025-12-31', end_date)
                    ) + 1 AS FLOAT
                  ) / CAST(DATEDIFF(DAY, start_date, end_date) + 1 AS FLOAT)) * learner_job_hours.hours,
                 2) AS hours
         FROM workforce.dbo.workforce_map
           INNER JOIN workforce.dbo.learner_job_hours ON workforce_map.learner_id = learner_job_hours.learner_id
         WHERE end_date >= '2025-01-01' AND
               start_date <= '2025-12-31' AND
               learner_job_hours.hours > 0
         UNION
         SELECT
           workforce_map.id,
           'Volunteer' AS source,
           volunteer_job_hours.job_title,
           volunteer_job_hours.uds_line,
           volunteer_job_hours.hcai_line,
           volunteer_job_hours.start_date,
           volunteer_job_hours.end_date,
           volunteer_job_hours.hours
         FROM workforce.dbo.workforce_map
           INNER JOIN workforce.dbo.volunteer_job_hours ON workforce_map.volunteer_id = volunteer_job_hours.volunteer_id
         WHERE end_date >= '2025-01-01' AND
               start_date <= '2025-12-31' AND
               volunteer_job_hours.hours > 0
       ),
     view_workforce AS (
         SELECT
           workforce_map.id,
           CASE
             WHEN employee_id IS NOT NULL THEN CONCAT(employees.legal_first_name, ' ', employees.legal_last_name)
             WHEN common_name LIKE '% %' AND PATINDEX('%[0-9]%', common_name) = 0 THEN common_name
             WHEN contractor_id IS NOT NULL THEN CONCAT(contractors.first_name, ' ', contractors.last_name)
             WHEN learner_id IS NOT NULL THEN CONCAT(learners.first_name, ' ', learners.last_name)
             WHEN volunteer_id IS NOT NULL
               THEN CONCAT(volunteers.first_name, ' ', volunteers.last_name) END AS full_name,
           COALESCE(network_users.primary_email, network_users.email, network_users.network_email,
                    employees.work_email, contractors.email, learners.email,
                    volunteers.email) AS email
         FROM workforce.dbo.workforce_map
           LEFT JOIN workforce.dbo.network_users ON workforce_map.network_id = network_users.id
           LEFT JOIN workforce.dbo.employees ON workforce_map.employee_id = employees.id
           LEFT JOIN workforce.dbo.contractors ON workforce_map.contractor_id = contractors.id
           LEFT JOIN workforce.dbo.learners ON workforce_map.learner_id = learners.id
           LEFT JOIN workforce.dbo.volunteers ON workforce_map.volunteer_id = volunteers.id
       )
SELECT
  view_workforce.id,
  view_workforce.full_name,
  view_workforce.email,
  total_workforce_hours.source,
  total_workforce_hours.job_title,
  total_workforce_hours.uds_line,
  total_workforce_hours.hcai_line,
  total_workforce_hours.start_date,
  total_workforce_hours.end_date,
  ROUND(total_workforce_hours.hours, 2) AS current_calendar_year_hours,
  ROUND(CAST((total_workforce_hours.hours) AS FLOAT) / (2080), 2) AS current_calendar_year_fte,
  ROUND(CAST((total_workforce_hours.hours) AS FLOAT) / (current_fte_hours.count),
        2) AS projected_eoy_fte
FROM view_workforce
  INNER JOIN total_workforce_hours ON view_workforce.id = total_workforce_hours.id
  CROSS JOIN current_fte_hours;

DROP VIEW IF EXISTS view_employees;
CREATE VIEW view_employees AS
WITH current_year_hours AS
       (
         SELECT
           employee_id,
           ROUND(CAST(SUM(paid_minutes) AS FLOAT) / 60, 2) AS current_year_hours,
           ROUND(CAST(SUM(paid_minutes) AS FLOAT) / (60 * 2080), 2) AS current_year_fte
         FROM workforce.dbo.employee_hours
         WHERE YEAR(date) = 2025
         GROUP BY employee_id
       ),
     previous_year_hours AS
       (
         SELECT
           employee_id,
           ROUND(CAST(SUM(paid_minutes) AS FLOAT) / (60 * 2080), 2) AS previous_year_fte
         FROM workforce.dbo.employee_hours
         WHERE YEAR(date) = 2024
         GROUP BY employee_id
       )
SELECT
  workforce_map.id,
  employees.id AS employee_id,
  CONCAT_WS(' ', employees.legal_first_name,
            LEFT(employees.legal_middle_name, 1),
            employees.legal_last_name) AS full_name,
  employees.work_email,
  COALESCE(locations.epic_location, locations.location) AS location,
  departments.name AS department,
  employee_job_titles.job_title,
  employee_job_titles.uds_line,
  employee_job_titles.hcai_line,
  employee_job_titles.has_clinical_admin_time,
  employees.status,
  employees.employment_type,
  employees.hire_date,
  employees.start_date,
  employees.termination_date,
  current_year_hours.current_year_hours,
  current_year_hours.current_year_fte,
  previous_year_hours.previous_year_fte,
  employees.hired_fte,
  CONCAT_WS(' ', manager.legal_first_name,
            LEFT(manager.legal_middle_name, 1),
            manager.legal_last_name) AS manager,
  employees.is_manager
FROM workforce.dbo.employees
  LEFT JOIN workforce.dbo.workforce_map ON employees.id = workforce_map.employee_id
  LEFT JOIN workforce.dbo.employees AS manager ON employees.reports_to_id = manager.id
  LEFT JOIN workforce.dbo.departments ON employees.department_id = departments.id
  LEFT JOIN workforce.dbo.locations ON employees.location_id = locations.id
  LEFT JOIN workforce.dbo.employee_job_titles ON employees.job_title_id = employee_job_titles.id
  LEFT JOIN current_year_hours ON employees.id = current_year_hours.employee_id
  LEFT JOIN previous_year_hours ON employees.id = previous_year_hours.employee_id;

DROP VIEW IF EXISTS view_employee_job_history;
CREATE VIEW view_employee_job_history AS
SELECT
  workforce_map.id,
  employees.id AS employee_id,
  CONCAT_WS(' ', employees.legal_first_name,
            LEFT(employees.legal_middle_name, 1),
            employees.legal_last_name) AS full_name,
  employee_job_history.job_title_id,
  employee_job_titles.job_title,
  employee_job_titles.uds_line,
  employee_job_titles.hcai_line,
  employee_job_history.start_date,
  employee_job_history.end_date,
  employees.status,
  employee_job_history.inserted_at
FROM workforce.dbo.employees
  LEFT JOIN workforce.dbo.workforce_map ON employees.id = workforce_map.employee_id
  INNER JOIN workforce.dbo.employee_job_history ON employees.id = employee_job_history.employee_id
  INNER JOIN workforce.dbo.employee_job_titles ON employee_job_history.job_title_id = employee_job_titles.id;

DROP VIEW IF EXISTS view_contractors;
CREATE VIEW view_contractors AS
WITH contractor_job_dates AS
       (
         SELECT
           contractor_id,
           MIN(start_date) AS start_date,
           MAX(end_date) AS end_date
         FROM workforce.dbo.contractor_job_hours
         GROUP BY contractor_id
       ),
     current_year_hours AS
       (
         SELECT
           contractor_id,
           SUM(hours) AS current_year_hours,
           (CAST(SUM(hours) AS float) / 2080.0) AS current_year_fte
         FROM workforce.dbo.contractor_job_hours
         WHERE YEAR(start_date) = 2025
         GROUP BY contractor_id
       ),
     previous_year_hours AS
       (
         SELECT
           contractor_id,
           (CAST(SUM(hours) AS float) / 2080.0) AS previous_year_fte
         FROM workforce.dbo.contractor_job_hours
         WHERE YEAR(start_date) = 2024
         GROUP BY contractor_id
       )
SELECT
  workforce_map.id,
  contractors.id AS contractor_id,
  CONCAT_WS(' ', contractors.first_name,
            contractors.last_name) AS contractor,
  network_users.common_name AS full_name,
  COALESCE(network_users.primary_email, network_users.email, network_users.network_email,
           contractors.email) AS email,
  contractors.vendor,
  contractors.job_title,
  contractors.uds_line,
  contractors.hcai_line,
  contractor_job_dates.start_date,
  contractor_job_dates.end_date,
  ROUND(CASE
          WHEN current_year_hours.contractor_id = 24
            THEN current_year_hours.current_year_hours / 5.0
          WHEN current_year_hours.contractor_id = 23
            THEN current_year_hours.current_year_hours / 4.0
          ELSE
            current_year_hours.current_year_hours
          END, 2) AS current_year_hours,
  ROUND(CASE
          WHEN current_year_hours.contractor_id = 24
            THEN current_year_hours.current_year_fte / 5.0
          WHEN current_year_hours.contractor_id = 23
            THEN current_year_hours.current_year_fte / 4.0
          ELSE
            current_year_hours.current_year_fte
          END, 2) AS current_year_fte,
  ROUND(CASE
          WHEN previous_year_hours.contractor_id = 24
            THEN previous_year_hours.previous_year_fte / 5.0
          WHEN previous_year_hours.contractor_id = 23
            THEN previous_year_hours.previous_year_fte / 4.0
          ELSE
            previous_year_hours.previous_year_fte
          END, 2) AS previous_year_fte
FROM workforce.dbo.contractors
  LEFT JOIN workforce.dbo.workforce_map ON contractors.id = workforce_map.contractor_id
  LEFT JOIN workforce.dbo.network_users ON workforce_map.network_id = network_users.id
  LEFT JOIN contractor_job_dates ON contractors.id = contractor_job_dates.contractor_id
  LEFT JOIN current_year_hours ON contractors.id = current_year_hours.contractor_id
  LEFT JOIN previous_year_hours ON contractors.id = previous_year_hours.contractor_id;

DROP VIEW IF EXISTS view_learners;
CREATE VIEW view_learners AS
WITH current_year_hours AS (
  SELECT
    learners.id AS learner_id,
    IIF(start_date < '2025-01-01', '2025-01-01', start_date) AS adjusted_start_date,
    IIF(end_date > '2025-12-31', '2025-12-31', end_date) AS adjusted_end_date,
    DATEDIFF(DAY,
             IIF(start_date < '2025-01-01', '2025-01-01', start_date),
             IIF(end_date > '2025-12-31', '2025-12-31', end_date)
    ) + 1 AS calendar_year_days,
    DATEDIFF(DAY, start_date, end_date) + 1 AS total_days,
    ROUND(CAST(
            DATEDIFF(DAY,
                     IIF(start_date < '2025-01-01', '2025-01-01', start_date),
                     IIF(end_date > '2025-12-31', '2025-12-31', end_date)
            ) + 1 AS FLOAT
          ) / CAST(DATEDIFF(DAY, start_date, end_date) + 1 AS FLOAT), 2) AS percentage_current_year,
    hours AS raw_hours,
    ROUND((CAST(
             DATEDIFF(DAY,
                      IIF(start_date < '2025-01-01', '2025-01-01', start_date),
                      IIF(end_date > '2025-12-31', '2025-12-31', end_date)
             ) + 1 AS FLOAT
           ) / CAST(DATEDIFF(DAY, start_date, end_date) + 1 AS FLOAT)) * COALESCE(learner_job_hours.hours, 0),
          2) AS current_year_hours,
    ROUND(((CAST(
              DATEDIFF(DAY,
                       IIF(start_date < '2025-01-01', '2025-01-01', start_date),
                       IIF(end_date > '2025-12-31', '2025-12-31', end_date)
              ) + 1 AS FLOAT
            ) / CAST(DATEDIFF(DAY, start_date, end_date) + 1 AS FLOAT)) * COALESCE(learner_job_hours.hours, 0)) /
          2080.0,
          2) AS current_year_fte
  FROM workforce.dbo.learners
    LEFT JOIN workforce.dbo.learner_job_hours
              ON learners.id = learner_job_hours.learner_id
  WHERE learner_job_hours.end_date >= '2025-01-01' AND
        learner_job_hours.start_date <= '2025-12-31' AND
        COALESCE(HOURS, 0) > 0
)
SELECT
  workforce_map.id,
  learners.id AS learner_id,
  CONCAT_WS(' ', learners.first_name, learners.last_name) AS full_name,
  COALESCE(network_users.primary_email, network_users.email,
           network_users.network_email, learners.email) AS email,
  learners.job_title,
  learners.site,
  learners.supervisor,
  learners.uds_line,
  learners.hcai_line,
  learner_job_hours.start_date,
  learner_job_hours.end_date,
  learner_job_hours.pre_graduate,
  learner_job_hours.post_graduate,
  learner_job_hours.workforce_role,
  current_year_hours.adjusted_start_date,
  current_year_hours.adjusted_end_date,
--   current_year_hours.calendar_year_days,
--   current_year_hours.total_days,
--   current_year_hours.percentage_current_year,
--   current_year_hours.raw_hours,
  current_year_hours.current_year_hours,
  current_year_hours.current_year_fte
FROM workforce.dbo.learners
  LEFT JOIN workforce.dbo.workforce_map ON learners.id = workforce_map.learner_id
  LEFT JOIN workforce.dbo.learner_job_hours
            ON learners.id = learner_job_hours.learner_id
  LEFT JOIN current_year_hours ON learners.id = current_year_hours.learner_id
  LEFT JOIN workforce.dbo.network_users ON workforce_map.network_id = network_users.id;

DROP VIEW IF EXISTS view_volunteers;
CREATE VIEW view_volunteers AS
WITH current_year_hours AS (
  SELECT
    volunteers.id AS volunteer_id,
    volunteer_job_hours.hours AS current_year_hours,
    ROUND(CAST(volunteer_job_hours.hours AS FLOAT) / 2080.0, 2) AS current_year_fte
  FROM workforce.dbo.volunteers
    LEFT JOIN workforce.dbo.volunteer_job_hours
              ON volunteers.id = volunteer_job_hours.volunteer_id
  WHERE volunteer_job_hours.end_date >= '2025-01-01' AND
        volunteer_job_hours.start_date <= '2025-12-31' AND
        COALESCE(hours, 0) > 0
)
SELECT
  workforce_map.id,
  volunteers.id AS volunteer_id,
  CONCAT_WS(' ', volunteers.first_name, volunteers.last_name) AS full_name,
  COALESCE(network_users.primary_email, network_users.email,
           network_users.network_email, volunteers.email) AS email,
  volunteers.job_title,
  volunteers.supervisor,
  volunteers.is_event_only,
  volunteers.uds_line,
  volunteers.hcai_line,
  volunteer_job_hours.start_date,
  volunteer_job_hours.end_date,
  volunteer_job_hours.pre_graduate,
  volunteer_job_hours.post_graduate,
  volunteer_job_hours.workforce_role,
  current_year_hours.current_year_hours,
  current_year_hours.current_year_fte
FROM workforce.dbo.volunteers
  LEFT JOIN workforce.dbo.workforce_map ON volunteers.id = workforce_map.volunteer_id
  LEFT JOIN workforce.dbo.volunteer_job_hours
            ON volunteers.id = volunteer_job_hours.volunteer_id AND volunteers.job_title = volunteer_job_hours.job_title
  LEFT JOIN current_year_hours ON volunteers.id = current_year_hours.volunteer_id
  LEFT JOIN workforce.dbo.network_users ON workforce_map.network_id = network_users.id;
