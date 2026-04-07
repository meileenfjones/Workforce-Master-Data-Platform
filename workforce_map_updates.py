import config
from pykeepass import PyKeePass
import pandas as pd
from sql_connection import SQLconnection
from analytics_platform_connection import AnalyticsPlatformConnection
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
            subject="Workforce - Workforce Map Update Script Error",
            msg=error_message,
            email_to=email_address,
        )


try:
    # Connect to Analytics Platform database
    keepass_db = PyKeePass(filename=config.KDBX_FILE, keyfile=config.KEY_PATH)
    analytics_production = AnalyticsPlatformConnection(keepass_db=keepass_db, keepass_entry=ssl_credentials)
    analytics_production.create_analytics_platform_connection(use_ssl=True)
    engine = analytics_production.can_connect()
    # Connect to Workforce database
    workforce_db_engine = SQLconnection(keepass_filename=config.KDBX_FILE, keepass_keyfile=config.KEY_PATH,
                                             return_engine=True)
except:
    error = 'Failure connecting to Analytics Platform and Workforce Database'
    print(error)
    analytics_production.kill_connection()
    workforce_db_engine.dispose()
    prefect_email(error)
    sys.exit()

try:
    workforce_insert_count_query = """
    SELECT
      COUNT(*) AS count
    FROM workforce.dbo.network_users
      INNER JOIN workforce.dbo.employees ON (TRIM(LOWER(employees.work_email)) = TRIM(LOWER(network_users.network_email)) OR
                                             TRIM(LOWER(employees.work_email)) = TRIM(LOWER(network_users.email)) OR
                                             TRIM(LOWER(employees.work_email)) = TRIM(LOWER(network_users.primary_email)))
    WHERE is_non_user = 0 AND
          is_primary_acct = 1 AND
          network_users.id NOT IN (
            SELECT
              network_id
            FROM workforce.dbo.workforce_map
            WHERE network_id IS NOT NULL
          ) AND
          employees.id NOT IN (
            SELECT
              employee_id
            FROM workforce.dbo.workforce_map
            WHERE employee_id IS NOT NULL
          );
          """

    workforce_insert_count = int(pd.read_sql(sql=workforce_insert_count_query, con=workforce_db_engine).iloc[0, 0])

    workforce_map_insert = """
    INSERT INTO workforce.dbo.workforce_map (network_id, employee_id)
    SELECT
      network_users.id AS network_id,
      employees.id AS employee_id
    FROM workforce.dbo.network_users
      INNER JOIN workforce.dbo.employees ON (TRIM(LOWER(employees.work_email)) = TRIM(LOWER(network_users.network_email)) OR
                                             TRIM(LOWER(employees.work_email)) = TRIM(LOWER(network_users.email)) OR
                                             TRIM(LOWER(employees.work_email)) = TRIM(LOWER(network_users.primary_email)))
    WHERE is_non_user = 0 AND
          is_primary_acct = 1 AND
          network_users.id NOT IN (
            SELECT
              network_id
            FROM workforce.dbo.workforce_map
            WHERE network_id IS NOT NULL
          ) AND
          employees.id NOT IN (
            SELECT
              employee_id
            FROM workforce.dbo.workforce_map
            WHERE employee_id IS NOT NULL
          );
          """

    with workforce_db_engine.connect() as connection:
        with connection.begin():
            connection.execute(text(workforce_map_insert))
    print(f'Newly mapped workforce: {workforce_insert_count}')
except:
    error = 'Failure making new Workforce matches between Network Users and Employees'
    print(error)
    analytics_production.kill_connection()
    workforce_db_engine.dispose()
    prefect_email(error)
    sys.exit()

try:
    # query rdm.users and import into Workforce schema
    analytics_users_query = """
      SELECT
        id,
        first_name,
        last_name,
        email,
        created_at,
        updated_at,
        active
      FROM rdm.users;
      """
    analytics_users = pd.DataFrame(pd.read_sql(sql=analytics_users_query, con=engine))
    analytics_users.to_sql(con=workforce_db_engine, name='#analytics_users_temp', if_exists='replace', index=False)

    analytics_users_update_count_query = """
        WITH final_updates AS
               (
                 SELECT
                   workforce_map.id,
                   rdm_users_temp.id AS analytics_id,
                   network_users.common_name,
                   CONCAT(rdm_users_temp.first_name, ' ', rdm_users_temp.last_name) AS user_name
                 FROM #analytics_users_temp AS rdm_users_temp
                   INNER JOIN workforce.dbo.network_users
                              ON TRIM(LOWER(rdm_users_temp.email)) = TRIM(LOWER(network_users.network_email)) OR
                                 TRIM(LOWER(rdm_users_temp.email)) = TRIM(LOWER(network_users.email)) OR
                                 TRIM(LOWER(rdm_users_temp.email)) = TRIM(LOWER(network_users.primary_email))
                   INNER JOIN workforce.dbo.workforce_map ON network_users.id = workforce_map.network_id
                 WHERE (workforce_map.analytics_id IS NULL OR
                        workforce_map.analytics_id != rdm_users_temp.id) AND
                       rdm_users_temp.id NOT IN (
                         SELECT
                           analytics_id
                         FROM workforce.dbo.workforce_map
                         WHERE analytics_id IS NOT NULL
                       )
               )
        SELECT
          COUNT(*) AS count
        FROM final_updates;
          """
    analytics_users_update_count = int(
        pd.read_sql(sql=analytics_users_update_count_query, con=workforce_db_engine).iloc[0, 0])

    analytics_users_update = """
        WITH final_updates AS
               (
                 SELECT
                   workforce_map.id,
                   rdm_users_temp.id AS analytics_id,
                   network_users.common_name,
                   CONCAT(rdm_users_temp.first_name, ' ', rdm_users_temp.last_name) AS user_name
                 FROM #analytics_users_temp AS rdm_users_temp
                   INNER JOIN workforce.dbo.network_users
                              ON TRIM(LOWER(rdm_users_temp.email)) = TRIM(LOWER(network_users.network_email)) OR
                                 TRIM(LOWER(rdm_users_temp.email)) = TRIM(LOWER(network_users.email)) OR
                                 TRIM(LOWER(rdm_users_temp.email)) = TRIM(LOWER(network_users.primary_email))
                   INNER JOIN workforce.dbo.workforce_map ON network_users.id = workforce_map.network_id
                 WHERE (workforce_map.analytics_id IS NULL OR
                        workforce_map.analytics_id != rdm_users_temp.id) AND
                       rdm_users_temp.id NOT IN (
                         SELECT
                           analytics_id
                         FROM workforce.dbo.workforce_map
                         WHERE analytics_id IS NOT NULL
                       )
               )
    UPDATE workforce.dbo.workforce_map
    SET analytics_id = final_updates.analytics_id
    FROM final_updates
    WHERE workforce_map.id = final_updates.id;
          """
    with workforce_db_engine.connect() as connection:
        with connection.begin():
            connection.execute(text(analytics_users_update))
    print(f'Staff updated with analytics User ID: {analytics_users_update_count}')
except:
    error = 'Failure updating workforce_map with analytics_id'
    print(error)
    analytics_production.kill_connection()
    workforce_db_engine.dispose()
    prefect_email(error)
    sys.exit()

try:
    epic_id_update_count_query = """
    WITH main_epic_ids AS
           (
             SELECT
               emp.user_id,
               emp_demo.email,
               emp.prov_id,
               emp.name AS epic_name,
               LOWER(LTRIM(RTRIM(SUBSTRING(emp.name, CHARINDEX(',', emp.name) + 1, LEN(emp.name))))) AS first_name,
               LOWER(LEFT(emp.name, CHARINDEX(',', emp.name) - 1)) AS last_name,
               CONCAT(LOWER(LTRIM(RTRIM(SUBSTRING([NAME], CHARINDEX(',', emp.name) + 1, LEN(emp.name))))), ' ',
                      LOWER(LEFT(emp.name, CHARINDEX(',', emp.name) - 1))) reformatted_name,
               emp.user_status_c,
               emp.last_pw_update,
               ROW_NUMBER()
                 OVER (PARTITION BY name ORDER BY IIF(emp_demo.email IS NOT NULL, 0, 1), user_status_c,LAST_PW_UPDATE DESC) AS row_number
             FROM Clarity_SA.dbo.clarity_emp_view AS emp
               INNER JOIN Clarity_SA.dbo.clarity_emp_demo AS emp_demo ON emp.user_id = emp_demo.user_id
             WHERE emp.user_id != 'SA000'
           ),
         unmapped_epic_users AS
           (
             SELECT
               user_id,
               email,
               prov_id,
               epic_name,
               first_name,
               last_name,
               reformatted_name
             FROM main_epic_ids
             WHERE row_number = 1 AND
                   user_id NOT IN (
                     SELECT
                       CAST(epic_id AS varchar(18))
                     FROM workforce.dbo.workforce_map
                     WHERE epic_id IS NOT NULL
                   )
           ),
         final_update AS
           (
             SELECT
               workforce_map.id,
               unmapped_epic_users.user_id AS new_epic_id,
               unmapped_epic_users.email,
               unmapped_epic_users.reformatted_name AS name
             FROM unmapped_epic_users
               INNER JOIN workforce.dbo.network_users
                          ON TRIM(LOWER(unmapped_epic_users.email)) = TRIM(LOWER(network_users.network_email)) OR
                             TRIM(LOWER(unmapped_epic_users.email)) = TRIM(LOWER(network_users.email)) OR
                             TRIM(LOWER(unmapped_epic_users.email)) = TRIM(LOWER(network_users.primary_email)) OR
                             TRIM(LOWER(unmapped_epic_users.reformatted_name)) = TRIM(LOWER(common_name)) OR
                             (TRIM(LOWER(unmapped_epic_users.first_name)) = TRIM(LOWER(network_users.first_name)) AND
                              TRIM(LOWER(unmapped_epic_users.last_name)) = TRIM(LOWER(network_users.last_name)))
               INNER JOIN workforce.dbo.workforce_map ON network_users.id = workforce_map.network_id
             WHERE workforce_map.epic_id IS NULL
           )
    SELECT
      COUNT(*) AS count
    FROM final_update;
          """
    epic_id_update_count = int(pd.read_sql(sql=epic_id_update_count_query, con=workforce_db_engine).iloc[0, 0])

    epic_id_update = """
    WITH main_epic_ids AS
           (
             SELECT
               emp.user_id,
               emp_demo.email,
               emp.prov_id,
               emp.name AS epic_name,
               LOWER(LTRIM(RTRIM(SUBSTRING(emp.name, CHARINDEX(',', emp.name) + 1, LEN(emp.name))))) AS first_name,
               LOWER(LEFT(emp.name, CHARINDEX(',', emp.name) - 1)) AS last_name,
               CONCAT(LOWER(LTRIM(RTRIM(SUBSTRING([NAME], CHARINDEX(',', emp.name) + 1, LEN(emp.name))))), ' ',
                      LOWER(LEFT(emp.name, CHARINDEX(',', emp.name) - 1))) reformatted_name,
               emp.user_status_c,
               emp.last_pw_update,
               ROW_NUMBER()
                 OVER (PARTITION BY name ORDER BY IIF(emp_demo.email IS NOT NULL, 0, 1), user_status_c,LAST_PW_UPDATE DESC) AS row_number
             FROM Clarity_SA.dbo.clarity_emp_view AS emp
               INNER JOIN Clarity_SA.dbo.clarity_emp_demo AS emp_demo ON emp.user_id = emp_demo.user_id
             WHERE emp.user_id != 'SA00'
           ),
         unmapped_epic_users AS
           (
             SELECT
               user_id,
               email,
               prov_id,
               epic_name,
               first_name,
               last_name,
               reformatted_name
             FROM main_epic_ids
             WHERE row_number = 1 AND
                   user_id NOT IN (
                     SELECT
                       CAST(epic_id AS varchar(18))
                     FROM workforce.dbo.workforce_map
                     WHERE epic_id IS NOT NULL
                   )
           ),
         final_update AS
           (
             SELECT
               workforce_map.id,
               unmapped_epic_users.user_id AS new_epic_id,
               unmapped_epic_users.email,
               unmapped_epic_users.reformatted_name AS name
             FROM unmapped_epic_users
               INNER JOIN workforce.dbo.network_users
                          ON TRIM(LOWER(unmapped_epic_users.email)) = TRIM(LOWER(network_users.network_email)) OR
                             TRIM(LOWER(unmapped_epic_users.email)) = TRIM(LOWER(network_users.email)) OR
                             TRIM(LOWER(unmapped_epic_users.email)) = TRIM(LOWER(network_users.primary_email)) OR
                             TRIM(LOWER(unmapped_epic_users.reformatted_name)) = TRIM(LOWER(common_name)) OR
                             (TRIM(LOWER(unmapped_epic_users.first_name)) = TRIM(LOWER(network_users.first_name)) AND
                              TRIM(LOWER(unmapped_epic_users.last_name)) = TRIM(LOWER(network_users.last_name)))
               INNER JOIN workforce.dbo.workforce_map ON network_users.id = workforce_map.network_id
             WHERE workforce_map.epic_id IS NULL
           )
    UPDATE workforce.dbo.workforce_map
    SET epic_id = new_epic_id
    FROM final_update
    WHERE workforce_map.id = final_update.id;
          """

    with workforce_db_engine.connect() as connection:
        with connection.begin():
            connection.execute(text(epic_id_update))
    print(f'Staff updated with EPIC User ID: {epic_id_update_count}')
except:
    error = 'Failure updating workforce_map with EPIC user_id'
    print(error)
    prefect_email(error)
finally:
    print('Successfully inserted/updated Workforce_map')
    analytics_production.kill_connection()
    workforce_db_engine.dispose()
    sys.exit()
