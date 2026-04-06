import config
from pykeepass import PyKeePass
import requests
import json
import pandas as pd
from adp_api_connection import AdpApiConnection
import adp_api_tools
from sql_connection import SQLconnection
from sqlalchemy.sql import text
import sys
from prefect import flow
from prefect_email import EmailServerCredentials, email_send_message


#######################################################################################################################

@flow
def prefect_email(error_message):
    email_server_credentials = EmailServerCredentials.load("prefect-ddbot-email")
    email_addresses = [email_to_notify]
    print(error_message)
    for email_address in email_addresses:
        email_send_message.with_options(name=f"email {email_address}").submit(
            email_server_credentials=email_server_credentials,
            subject="Workforce - ADP Workers Script Error",
            msg=error_message,
            email_to=email_address,
        )


try:
    workforce_db_engine = SQLconnection(keepass_filename=config.KDBX_FILE, keepass_keyfile=config.KEY_PATH,
                                             return_engine=True)
    # Initialize log
    log_dictionary = {'query_type': 'Workers',
                      'query_count': None,
                      'update_count': None,
                      'insert_count': None,
                      'failure_step': None,
                      'is_success': False}
except:
    error = 'Failure connecting to Workforce and initializing log'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    prefect_email(error)
    sys.exit()

try:
    keepass_db = PyKeePass(filename=config.KDBX_FILE, keyfile=config.KEY_PATH)
    # Each request to one of ADP's APIs needs to be accompanied by a header containing a bearer token issued
    # by the ADP Security Token Service which will be generated via AdpApiConnection class and be active for 1 hour
    api_connection = AdpApiConnection(keepass_db=keepass_db, keepass_entry=certifcate_credential_title)
    api_connection.generate_bearer_token()
except:
    error = 'Failure creating API bearer token'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    api_connection.kill_connection()
    prefect_email(error)
    sys.exit()

try:
    get_response = requests.get(f'https://api.adp.com/hr/v2/workers?$top=100', headers=api_connection.header,
                                data=api_connection.grant_type, cert=(api_connection.cert, api_connection.key))
    raw_worker_df = pd.json_normalize(json.loads(get_response.text)['workers'])
    if get_response.status_code != 200:
        raise Exception
    query_skips = 0
    print("Success connecting to ADP Workers table")
except:
    error = 'Failure connecting to ADP Workers table'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    api_connection.kill_connection()
    prefect_email(error)
    sys.exit()

try:
    worker_list = []
    while get_response.status_code == 200:  # 204 = no content
        workers_data = json.loads(get_response.text)
        for worker in workers_data["workers"]:
            worker_list.append({
                'id': adp_api_tools.get_id(worker),
                'aoid': adp_api_tools.get_aoid(worker),
                'location_id': adp_api_tools.get_location_id(worker),
                'location': adp_api_tools.get_location(worker),
                'department_id': adp_api_tools.get_department_id(worker),
                'department': adp_api_tools.get_department(worker),
                'reports_to_id': adp_api_tools.get_reports_to_id(worker),
                'job_title_id': adp_api_tools.get_job_title_id(worker),
                'job_title': adp_api_tools.get_job_title(worker),
                'legal_first_name': adp_api_tools.get_legal_first_name(worker),
                'legal_middle_name': adp_api_tools.get_legal_middle_name(worker),
                'legal_last_name': adp_api_tools.get_legal_last_name(worker),
                'preferred_first_name': adp_api_tools.get_preferred_first_name(worker),
                'preferred_middle_name': adp_api_tools.get_preferred_middle_name(worker),
                'preferred_last_name': adp_api_tools.get_preferred_last_name(worker),
                'gender': adp_api_tools.get_gender(worker),
                'pronouns': adp_api_tools.get_pronouns(worker),
                'work_email': adp_api_tools.get_work_email(worker),
                'personal_email': adp_api_tools.get_personal_email(worker),
                'personal_mobile': adp_api_tools.get_personal_mobile(worker),
                'status': adp_api_tools.get_status(worker),
                'employment_type': adp_api_tools.get_employment_type(worker),
                'actual_seniority_date': adp_api_tools.get_actual_seniority_date(worker),
                'seniority_date': adp_api_tools.get_seniority_date(worker),
                'original_hire_date': adp_api_tools.get_original_hire_date(worker),
                'hire_date': adp_api_tools.get_hire_date(worker),
                'ultimate_hire_date': adp_api_tools.get_ultimate_hire_date(worker),
                'start_date': adp_api_tools.get_start_date(worker),
                'evaluation_date': adp_api_tools.get_evaluation_date(worker),
                'termination_date': adp_api_tools.get_termination_date(worker),
                'hired_fte': adp_api_tools.get_hired_fte(worker),
                'is_manager': adp_api_tools.get_is_manager(worker),
                # employee sensitive info added for imprivata SFTP send
                'dob': adp_api_tools.get_dob(worker),
                'address_line_1': adp_api_tools.get_address_line_1(worker),
                'address_line_2': adp_api_tools.get_address_line_2(worker),
                'city': adp_api_tools.get_city(worker),
                'state': adp_api_tools.get_state(worker),
                'zip_code': adp_api_tools.get_zip_code(worker)
            })
        query_skips = query_skips + 100
        get_response = requests.get(f'https://api.adp.com/hr/v2/workers?$top=100&$skip={query_skips}',
                                    headers=api_connection.header, data=api_connection.grant_type,
                                    cert=(api_connection.cert, api_connection.key))
        print(len(worker_list))
    adp_workers_df = pd.DataFrame(worker_list)
    print("Success querying selected columns of Workers table: " + str(len(worker_list)) + " entries")
    log_dictionary['query_count'] = len(worker_list)
    api_connection.kill_connection()
except:
    error = 'Failure querying selected columns of Workers table'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    api_connection.kill_connection()
    prefect_email(error)
    sys.exit()

try:
    adp_workers_df.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_workers', if_exists='replace',
                          index=False)
    print('Successfully replaced adp_workers')
except:
    error = 'Failure pushing data to adp_workers'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    prefect_email(error)
    sys.exit()

try:
    # update tables with new department, location, or job title mappings
    unmapped_departments_query = """
    SELECT
      department_id
    FROM workforce.dbo.adp_workers
    WHERE department_id IS NOT NULL
    EXCEPT
    SELECT
      id
    FROM workforce.dbo.departments
    """
    unmapped_departments = pd.read_sql(sql=unmapped_departments_query,
                                       con=workforce_db_engine).values.flatten().tolist()
    if len(unmapped_departments) != 0:
        insert_new_departments = """
        INSERT INTO workforce.dbo.departments (id, name)
        SELECT DISTINCT
          department_id,
          department
        FROM workforce.dbo.adp_workers
        WHERE department_id IS NOT NULL AND
              department_id NOT IN (
                SELECT id
                FROM departments
              );"""
        with workforce_db_engine.connect() as connection:
            with connection.begin():
                connection.execute(text(insert_new_departments))
        print('Successfully inserted new departments')

    unmapped_locations_query = """
    SELECT
      location_id
    FROM workforce.dbo.adp_workers
    WHERE location_id IS NOT NULL
    EXCEPT
    SELECT
      id
    FROM workforce.dbo.locations;
    """
    unmapped_locations = pd.read_sql(sql=unmapped_locations_query, con=workforce_db_engine).values.flatten().tolist()
    if len(unmapped_locations) != 0:
        insert_new_locations = """
        INSERT INTO workforce.dbo.locations (id, location)
        SELECT DISTINCT
          location_id,
          location
        FROM workforce.dbo.adp_workers
        WHERE location_id IS NOT NULL AND
              location_id NOT IN (
                SELECT
                  id
                FROM locations
              );"""
        with workforce_db_engine.connect() as connection:
            with connection.begin():
                connection.execute(text(insert_new_locations))
        print('Successfully inserted new locations')

    unmapped_job_titles_query = """
    SELECT
      CONCAT(job_title_id, ': ', job_title)
    FROM workforce.dbo.adp_workers
    WHERE CONCAT(job_title_id, ': ', job_title) IS NOT NULL
    EXCEPT
    SELECT
      CONCAT(job_title_id, ': ', job_title)
    FROM workforce.dbo.employee_job_titles;
    """
    unmapped_job_titles = pd.read_sql(sql=unmapped_job_titles_query, con=workforce_db_engine).values.flatten().tolist()
    if len(unmapped_job_titles) != 0:
        insert_new_job_titles = """
        INSERT INTO workforce.dbo.employee_job_titles (job_title_id, job_title)
        SELECT
          job_title_id,
          job_title
        FROM workforce.dbo.adp_workers
        WHERE job_title_id IS NOT NULL AND
              job_title IS NOT NULL
        EXCEPT
        SELECT
          job_title_id,
          job_title
        FROM workforce.dbo.employee_job_titles;
        """
        with workforce_db_engine.connect() as connection:
            with connection.begin():
                connection.execute(text(insert_new_job_titles))
        print('Successfully inserted new job titles')
except:
    error = 'Failure checking for new departments, locations, or job titles'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    prefect_email(error)
    sys.exit()

try:
    # update employee info, insert new employees, and update deleted flags for employees deleted from ADP
    employees_with_updates = """
    SELECT
      COUNT(*) AS count
    FROM workforce.dbo.adp_workers
      INNER JOIN workforce.dbo.employees ON adp_workers.aoid = employees.aoid
      INNER JOIN workforce.dbo.employee_info ON adp_workers.aoid = employee_info.aoid
      INNER JOIN workforce.dbo.employee_job_titles ON adp_workers.job_title_id = employee_job_titles.job_title_id
      AND adp_workers.job_title = employee_job_titles.job_title
    WHERE adp_workers.id != employees.id OR
          adp_workers.location_id != employees.location_id OR
          adp_workers.department_id != employees.department_id OR
          adp_workers.reports_to_id != employees.reports_to_id OR
          employee_job_titles.id != employees.job_title_id OR
          adp_workers.legal_first_name != employees.legal_first_name OR
          adp_workers.legal_middle_name != employees.legal_middle_name OR
          adp_workers.legal_last_name != employees.legal_last_name OR
          adp_workers.preferred_first_name != employees.preferred_first_name OR
          adp_workers.preferred_middle_name != employees.preferred_middle_name OR
          adp_workers.preferred_last_name != employees.preferred_last_name OR
          adp_workers.gender != employees.gender OR
          adp_workers.pronouns != employees.pronouns OR
          adp_workers.work_email != employees.work_email OR
          adp_workers.personal_email != employees.personal_email OR
          adp_workers.personal_mobile != employees.personal_mobile OR
          adp_workers.status != employees.status OR
          adp_workers.employment_type != employees.employment_type OR
          adp_workers.hire_date != employees.hire_date OR
          adp_workers.start_date != employees.start_date OR
          adp_workers.evaluation_date != employees.evaluation_date OR
          adp_workers.termination_date != employees.termination_date OR
          adp_workers.hired_fte != employees.hired_fte OR
          adp_workers.is_manager != employees.is_manager OR
          adp_workers.dob != employee_info.dob OR
          adp_workers.address_line_1 != employee_info.address_line_1 OR
          adp_workers.address_line_2 != employee_info.address_line_2 OR
          adp_workers.city != employee_info.city OR
          adp_workers.state != employee_info.state OR
          adp_workers.zip_code != employee_info.zip_code
          ;"""

    updated_employees_count = int(pd.read_sql(sql=employees_with_updates, con=workforce_db_engine).iloc[0, 0])

    update_employees = """
    UPDATE workforce.dbo.employees
    SET id                    = workers.id,
        location_id           = workers.location_id,
        department_id         = workers.department_id,
        reports_to_id         = workers.reports_to_id,
        job_title_id          = employee_job_titles.id,
        legal_first_name      = workers.legal_first_name,
        legal_middle_name     = workers.legal_middle_name,
        legal_last_name       = workers.legal_last_name,
        preferred_first_name  = workers.preferred_first_name,
        preferred_middle_name = workers.preferred_middle_name,
        preferred_last_name   = workers.preferred_last_name,
        gender                = workers.gender,
        pronouns              = workers.pronouns,
        work_email            = workers.work_email,
        personal_email        = workers.personal_email,
        personal_mobile       = workers.personal_mobile,
        status                = workers.status,
        employment_type       = workers.employment_type,
        hire_date             = workers.hire_date,
        start_date            = workers.start_date,
        evaluation_date       = workers.evaluation_date,
        termination_date      = workers.termination_date,
        hired_fte             = workers.hired_fte,
        is_manager            = workers.is_manager
    FROM workforce.dbo.adp_workers AS workers
      INNER JOIN workforce.dbo.employee_job_titles ON workers.job_title_id = employee_job_titles.job_title_id
      AND workers.job_title = employee_job_titles.job_title
    WHERE employees.aoid = workers.aoid;
    """

    update_employee_info = """
    UPDATE workforce.dbo.employee_info
    SET id             = workers.id,
        dob            = workers.dob,
        address_line_1 = workers.address_line_1,
        address_line_2 = workers.address_line_2,
        city           = workers.city,
        state          = workers.state,
        zip_code       = workers.zip_code
    FROM workforce.dbo.adp_workers workers
    WHERE employee_info.aoid = workers.aoid;"""

    new_employees = """
        WITH new_employees AS
           (
             SELECT
               aoid
             FROM workforce.dbo.adp_workers
             EXCEPT
             SELECT
               aoid
             FROM workforce.dbo.employees
           )
    SELECT
      COUNT(*) AS count
    FROM new_employees;
    """

    new_employees_count = int(pd.read_sql(sql=new_employees, con=workforce_db_engine).iloc[0, 0])

    insert_new_employees = """
    INSERT INTO workforce.dbo.employees
    (id,
     aoid,
     location_id,
     department_id,
     reports_to_id,
     job_title_id,
     legal_first_name,
     legal_middle_name,
     legal_last_name,
     preferred_first_name,
     preferred_middle_name,
     preferred_last_name,
     gender,
     pronouns,
     work_email,
     personal_email,
     personal_mobile,
     status,
     employment_type,
     hire_date,
     start_date,
     evaluation_date,
     termination_date,
     hired_fte,
     is_manager)
    SELECT
      workers.id,
      aoid,
      location_id,
      department_id,
      reports_to_id,
      employee_job_titles.id,
      legal_first_name,
      legal_middle_name,
      legal_last_name,
      preferred_first_name,
      preferred_middle_name,
      preferred_last_name,
      gender,
      pronouns,
      work_email,
      personal_email,
      personal_mobile,
      status,
      employment_type,
      hire_date,
      start_date,
      evaluation_date,
      termination_date,
      hired_fte,
      is_manager
    FROM workforce.dbo.adp_workers AS workers
      INNER JOIN workforce.dbo.employee_job_titles ON workers.job_title_id = employee_job_titles.job_title_id
      AND workers.job_title = employee_job_titles.job_title
    WHERE aoid NOT IN (
      SELECT
        aoid
      FROM employees
    )
    ORDER BY aoid;
    """

    insert_new_employee_info = """
       INSERT INTO workforce.dbo.employee_info
       (id,
        aoid,
        dob,
        address_line_1,
        address_line_2,
        city,
        state,
        zip_code)
       SELECT
         id,
         aoid,
         dob,
         address_line_1,
         address_line_2,
         city,
         state,
         zip_code
       FROM workforce.dbo.adp_workers workers
       WHERE aoid NOT IN (
         SELECT aoid
         FROM employee_info
       )
       ORDER BY aoid;
       """

    update_deleted_employees = """
    UPDATE workforce.dbo.employees
    SET is_deleted = 1,
        deleted_at = GETDATE()
    WHERE aoid NOT IN (
      SELECT
        aoid
      FROM workforce.dbo.adp_workers
    ) AND is_deleted = 0;
          """

    with workforce_db_engine.connect() as connection:
        with connection.begin():
            connection.execute(text(update_employees))
            connection.execute(text(update_employee_info))
            connection.execute(text(insert_new_employees))
            connection.execute(text(insert_new_employee_info))
            connection.execute(text(update_deleted_employees))

    log_dictionary['update_count'] = updated_employees_count
    print(f'Updated Employees: {updated_employees_count}')
    log_dictionary['insert_count'] = new_employees_count
    print(f'Inserted Employees: {new_employees_count}')
    print('Successfully updated Employees table')
except:
    error = 'Failure updating Employees table'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    prefect_email(error)
    sys.exit()

try:
    # insert new employee jobs into history table and update end dates with positional end dates or termination dates
    insert_new_employee_job_history = """
    INSERT INTO workforce.dbo.employee_job_history (employee_id, job_title_id, start_date)
    SELECT
      id,
      job_title_id,
      start_date
    FROM employees
    EXCEPT
    SELECT
      employee_id,
      job_title_id,
      start_date
    FROM employee_job_history
    ORDER BY start_date;
          """

    # if employee is terminated, then use termination date as their last job title's end date
    update_employee_job_history_with_terminations = """
    UPDATE workforce.dbo.employee_job_history
    SET employee_job_history.end_date = employees.termination_date
    FROM employee_job_history
      INNER JOIN (
      SELECT
        employee_id,
        MAX(inserted_at) AS inserted_at
      FROM employee_job_history
      GROUP BY employee_id
    ) last_job ON employee_job_history.employee_id = last_job.employee_id AND
                  employee_job_history.inserted_at = last_job.inserted_at
      INNER JOIN workforce.dbo.employees ON employee_job_history.employee_id = employees.id
    WHERE employees.termination_date IS NOT NULL AND
          employee_job_history.end_date IS NULL;
          """

    # if employee changes job titles, use new job title's start date - 1 day as previous job's end date
    update_employee_job_history_with_position_end_dates = """
    WITH ranked_job_titles AS (
      SELECT
        id,
        employee_id,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY inserted_at DESC) AS rn
      FROM workforce.dbo.employee_job_history
    )
    UPDATE workforce.dbo.employee_job_history
    SET end_date = DATEADD(DAY, -1, current_job_title.start_date)
    FROM ranked_job_titles last_job_title
      INNER JOIN
    ranked_job_titles current_job_title
    ON last_job_title.employee_id = current_job_title.employee_id
    WHERE employee_job_history.id = last_job_title.id AND
          last_job_title.rn = 2 AND
          current_job_title.rn = 1 AND
          -- making sure that end date for a row is after start date
          employee_job_history.start_date < DATEADD(DAY, -1, current_job_title.start_date);
          """

    # due to some job title updates caused by county realignment and not true job changes, delete old entries
    delete_old_realignment_entries_from_employee_job_history = """
    WITH job_title_realignment AS (
      SELECT
        id,
        ROW_NUMBER() OVER (PARTITION BY employee_id, start_date ORDER BY inserted_at DESC) AS row_number
      FROM employee_job_history
    )
    DELETE
    FROM workforce.dbo.employee_job_history
    WHERE id IN (
      SELECT
        id
      FROM job_title_realignment
      WHERE row_number != 1
    );
    """

    drop_adp_workers = """DROP TABLE IF EXISTS workforce.dbo.adp_workers;"""

    with workforce_db_engine.connect() as connection:
        with connection.begin():
            connection.execute(text(insert_new_employee_job_history))
            connection.execute(text(update_employee_job_history_with_terminations))
            connection.execute(text(update_employee_job_history_with_position_end_dates))
            connection.execute(text(delete_old_realignment_entries_from_employee_job_history))
            connection.execute(text(drop_adp_workers))
    print('Successfully updated employee_job_history')
    log_dictionary['is_success'] = True
except:
    error = 'Failure updating employee_job_history'
    print(error)
    log_dictionary['failure_step'] = str(error)
    prefect_email(error)
finally:
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    sys.exit()
