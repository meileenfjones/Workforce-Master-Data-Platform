CREATE TABLE workforce.dbo.workforce_map
(
  id            INTEGER PRIMARY KEY IDENTITY (1,1),
  network_id    INTEGER,
  employee_id   INTEGER,
  contractor_id INTEGER,
  learner_id    INTEGER,
  volunteer_id  INTEGER,
  relevant_id   INTEGER,
  epic_id       INTEGER,
  ecw_ids       NVARCHAR(MAX)
);

CREATE TABLE workforce.dbo.network_users
(
  id               INTEGER PRIMARY KEY IDENTITY (1,1),
  guid             VARCHAR(36),
  primary_guid     VARCHAR(36),
  source           VARCHAR(3),
  first_name       VARCHAR(MAX),
  last_name        VARCHAR(MAX),
  common_name      VARCHAR(MAX),
  display_name     VARCHAR(MAX),
  network_email    VARCHAR(MAX),
  primary_email    VARCHAR(MAX),
  email            VARCHAR(MAX),
  job_title        VARCHAR(MAX),
  account_name     VARCHAR(MAX),
  active_dir_group VARCHAR(MAX),
  department       VARCHAR(MAX),
  location         VARCHAR(MAX),
  user_description VARCHAR(MAX),
  phone_extension  VARCHAR(MAX),
  mobile_phone     VARCHAR(MAX),
  created_at       SMALLDATETIME,
  inserted_at      SMALLDATETIME DEFAULT GETDATE(),
  is_primary_acct  BIT           DEFAULT 1,
  is_non_user      BIT           DEFAULT 0,
  is_disabled      BIT           DEFAULT 0,
  disabled_at      SMALLDATETIME,
  is_deleted       BIT           DEFAULT 0,
  deleted_at       SMALLDATETIME
);

CREATE TABLE workforce.dbo.employee_job_titles
(
  id                      INTEGER PRIMARY KEY IDENTITY (1,1),
  job_title_id            INTEGER      NOT NULL,
  job_title               VARCHAR(MAX) NOT NULL,
  uds_line                VARCHAR(4),
  hcai_line               VARCHAR(3),
  has_clinical_admin_time BIT          NOT NULL DEFAULT 0,
  inserted_at             SMALLDATETIME         DEFAULT GETDATE()
);

CREATE TABLE workforce.dbo.locations
(
  id               INTEGER PRIMARY KEY,
  location         VARCHAR(MAX) NOT NULL,
  epic_location_id INTEGER,
  epic_location    VARCHAR(MAX),
  county           VARCHAR(MAX)
);

CREATE TABLE workforce.dbo.departments
(
  id   INTEGER PRIMARY KEY,
  name VARCHAR(MAX) NOT NULL
);

-- historical + current employees
CREATE TABLE workforce.dbo.employees
(
  id                    INTEGER PRIMARY KEY, -- file_number
  aoid                  CHAR(16)     NOT NULL,
  location_id           INTEGER      NOT NULL REFERENCES workforce.dbo.locations (id),
  department_id         INTEGER      NOT NULL REFERENCES workforce.dbo.departments (id),
  reports_to_id         INTEGER,
  job_title_id          INTEGER      NOT NULL REFERENCES workforce.dbo.employee_job_titles (id),
  legal_first_name      VARCHAR(MAX) NOT NULL,
  legal_middle_name     VARCHAR(MAX),
  legal_last_name       VARCHAR(MAX) NOT NULL,
  preferred_first_name  VARCHAR(MAX),
  preferred_middle_name VARCHAR(MAX),
  preferred_last_name   VARCHAR(MAX),
  gender                VARCHAR(MAX),
  pronouns              VARCHAR(MAX),
  work_email            VARCHAR(MAX),
  personal_email        VARCHAR(MAX),
  personal_mobile       VARCHAR(14),
  status                VARCHAR(MAX),
  employment_type       VARCHAR(MAX),
--    actual_seniority_date DATE,
--    seniority_date DATE,
--    original_hire_date DATE,
  hire_date             DATE,                -- recalculate
  start_date            DATE,                -- recalculate
  evaluation_date       DATE,
  termination_date      DATE,
  hired_fte             DECIMAL(3,2),
  inserted_at           SMALLDATETIME DEFAULT GETDATE(),
  deleted_at            SMALLDATETIME,
  is_deleted            BIT DEFAULT 0,
  is_manager            BIT,
);

CREATE TABLE workforce.dbo.employee_info
(
  id             INTEGER  NOT NULL REFERENCES workforce.dbo.employees (id),
  aoid           CHAR(16) NOT NULL,
  dob            VARCHAR(MAX),
  address_line_1 VARCHAR(MAX),
  address_line_2 VARCHAR(MAX),
  city           VARCHAR(MAX),
  state          VARCHAR(2),
  zip_code       VARCHAR(5)
);

CREATE TABLE workforce.dbo.employee_job_history
(
  id INTEGER PRIMARY KEY IDENTITY (1,1),
  employee_id  INTEGER NOT NULL REFERENCES workforce.dbo.employees (id) ON UPDATE CASCADE,
  job_title_id INTEGER NOT NULL REFERENCES workforce.dbo.employee_job_titles (id),
  start_date   DATE    NOT NULL,
  end_date     DATE,
  inserted_at  SMALLDATETIME DEFAULT GETDATE()
);

CREATE TABLE workforce.dbo.employee_hours
(
  id              INTEGER IDENTITY (1,1),
  employee_id     INTEGER NOT NULL REFERENCES workforce.dbo.employees (id) ON UPDATE CASCADE,
  job_title_id    INTEGER NOT NULL REFERENCES workforce.dbo.employee_job_titles (id),
  date            DATE    NOT NULL,
  work_minutes    INTEGER NOT NULL,
  paid_minutes    INTEGER NOT NULL,
  regular_minutes INTEGER NOT NULL,
  PRIMARY KEY (employee_id, date)
);

CREATE TABLE workforce.dbo.timecard_dates
(
  period_start DATE NOT NULL,
  date         DATE NOT NULL
);

CREATE TABLE workforce.dbo.contractors
(
  id          INTEGER PRIMARY KEY IDENTITY (1,1),
  name        VARCHAR(MAX) NOT NULL,
  first_name  VARCHAR(MAX),
  last_name   VARCHAR(MAX),
  email       VARCHAR(MAX),
  job_title   VARCHAR(MAX),
  vendor      VARCHAR(MAX),
  location_id INTEGER REFERENCES workforce.dbo.locations (id) DEFAULT 0, --default
  inserted_at SMALLDATETIME                                   DEFAULT GETDATE(),
  uds_line    VARCHAR(4),
  hcai_line   VARCHAR(3)
);

CREATE TABLE workforce.dbo.contractor_job_hours
(
  id            INTEGER PRIMARY KEY IDENTITY (1,1),
  contractor_id INTEGER NOT NULL REFERENCES workforce.dbo.contractors (id) ON UPDATE CASCADE,
  invoice       VARCHAR(max),
  job_title     VARCHAR(MAX),
  start_date    DATE    NOT NULL,
  end_date      DATE,
  uds_line      VARCHAR(4),
  hcai_line     VARCHAR(3),
  hours         DECIMAL(6, 2),
  inserted_at   SMALLDATETIME DEFAULT GETDATE()
);

CREATE TABLE workforce.dbo.learners
(
  id          INTEGER PRIMARY KEY IDENTITY (1,1),
  external_id INTEGER      NOT NULL,
  name        VARCHAR(MAX) NOT NULL,
  first_name  VARCHAR(MAX),
  last_name   VARCHAR(MAX),
  email       VARCHAR(MAX),
  job_title   VARCHAR(MAX),
  site        VARCHAR(MAX),
  location_id INTEGER REFERENCES workforce.dbo.locations (id) DEFAULT 0,
  supervisor  VARCHAR(MAX),
  inserted_at SMALLDATETIME                                   DEFAULT GETDATE(),
  uds_line    VARCHAR(4),
  hcai_line   VARCHAR(3)
);

CREATE TABLE workforce.dbo.learner_job_hours
(
  id             INTEGER PRIMARY KEY IDENTITY (1,1),
  learner_id     INTEGER NOT NULL REFERENCES workforce.dbo.learners (id) ON UPDATE CASCADE,
  job_title      VARCHAR(MAX),
  start_date     DATE    NOT NULL,
  end_date       DATE,
  uds_line       VARCHAR(4),
  hcai_line      VARCHAR(3),
  pre_graduate   BIT           DEFAULT 0,
  post_graduate  BIT           DEFAULT 0,
  workforce_role VARCHAR(MAX),
  hours          DECIMAL(6, 2),
  inserted_at    SMALLDATETIME DEFAULT GETDATE()
);

CREATE TABLE workforce.dbo.volunteers
(
  id            INTEGER PRIMARY KEY IDENTITY (1,1),
  first_name    VARCHAR(MAX),
  last_name     VARCHAR(MAX),
  email         VARCHAR(MAX),
  job_title     VARCHAR(MAX),
  location_id   INTEGER REFERENCES workforce.dbo.locations (id)   DEFAULT 0, --default
  department_id INTEGER REFERENCES workforce.dbo.departments (id) DEFAULT 0, --default
  supervisor    VARCHAR(MAX),
  inserted_at   SMALLDATETIME                                     DEFAULT GETDATE(),
  external_id   BIT,
  is_event_only BIT,
  uds_line      VARCHAR(4),
  hcai_line     VARCHAR(3)
);

CREATE TABLE workforce.dbo.volunteer_job_hours
(
  id           INTEGER PRIMARY KEY IDENTITY (1,1),
  volunteer_id INTEGER NOT NULL REFERENCES workforce.dbo.volunteers (id) ON UPDATE CASCADE,
  job_title    VARCHAR(MAX),
  start_date   DATE    NOT NULL,
  end_date     DATE,
  uds_line     VARCHAR(4),
  hcai_line    VARCHAR(3),
  hours        DECIMAL(3, 2),
  inserted_at  SMALLDATETIME DEFAULT GETDATE()
);

CREATE TABLE workforce.dbo.holidays
(
  holiday VARCHAR(MAX),
  date    VARCHAR(MAX)
);
