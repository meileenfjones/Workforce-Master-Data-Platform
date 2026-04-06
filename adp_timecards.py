import config
from pykeepass import PyKeePass
import requests
import json
import pandas as pd
from adp_api_connection import AdpApiConnection
from sql_connection import SQLconnection
import adp_api_tools as timecards_json_parse
from sqlalchemy.sql import text
import time
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
            subject="Workforce - ADP Timecards Script Error",
            msg=error_message,
            email_to=email_address,
        )


try:
    workforce_db_engine = SQLconnection(keepass_filename=config.KDBX_FILE, keepass_keyfile=config.KEY_PATH,
                                             return_engine=True)
    # Initialize log
    log_dictionary = {'query_type': 'Timecards',
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
    # Each request to one of ADP's APIs needs to be accompanied by an Authorization header containing a bearer token issued
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

# query relevant Workers table for aoid list
try:
    # either active, on leave, or was terminated only in the last month
    workers_query = """
    SELECT
      id,
      aoid,
      job_title_id
    FROM workforce.dbo.employees
    WHERE status != 'Terminated' OR
          termination_date >= DATEADD(MONTH, -1, GETDATE());"""
    adp_workers_df = pd.read_sql(sql=workers_query, con=workforce_db_engine)
    # pull into dictionary and potentially append through timecards
    aoid_list = adp_workers_df['aoid'].tolist()
except:
    error = 'Failure getting aoid list'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    api_connection.kill_connection()
    prefect_email(error)
    sys.exit()

try:
    final_df = pd.DataFrame()
    for index, aoid in enumerate(aoid_list):
        # due to connection errors for querying this much data, reset every 200 queries
        if index in [100, 200, 300, 400, 500, 600, 700, 800, 900]:
            print(f'Querying employee {index} out of {len(aoid_list)}')
            api_connection = AdpApiConnection(keepass_db=keepass_db, keepass_entry=certifcate_credential_title)
            api_connection.generate_bearer_token()
        attempt = 0
        success = False
        while attempt < 4 and not success:
            get_response = requests.get(
                f"https://api.adp.com/time/v2/workers/{aoid}/time-cards?$filter=timeCards/periodCode/codeValue eq 'previous' or timeCards/periodCode/codeValue eq 'current'",
                headers=api_connection.header, data=api_connection.grant_type,
                cert=(api_connection.cert, api_connection.key))
            if get_response.status_code == 200:
                success = True
                timecard_data = json.loads(get_response.text)
                timecard_list = []
                for time_card in timecard_data["timeCards"]:
                    # print(time_card)
                    associate_oid = time_card["associateOID"]
                    employee_id = adp_workers_df[adp_workers_df["aoid"] == associate_oid]["id"].iloc[0]
                    job_title_id = adp_workers_df[adp_workers_df["aoid"] == associate_oid]["job_title_id"].iloc[0]
                    # print(employee_id)
                    period_start = time_card["timePeriod"]["startDate"]
                    for daily_total in time_card["dailyTotals"]:
                        # print(daily_total)
                        if 'entryDate' in daily_total.keys():
                            entry_date = daily_total["entryDate"]
                            # print(entry_date)
                            pay_code_name = daily_total["payCode"]["shortName"]
                            pay_code = daily_total["payCode"]["codeValue"]
                            # print(pay_code)
                            # time_duration = int(extract_time(daily_total['timeDuration']))
                            if daily_total['payCode']['codeValue'] in ['REGULAR', 'REGSAL']:
                                regular_minutes = int(
                                    timecards_json_parse.extract_minutes_from_timecards(daily_total['timeDuration']))
                            else:
                                regular_minutes = 0
                            if daily_total['payCode']['codeValue'] in ['REGULAR', 'REGSAL', 'OVERTIME', 'MKU', 'EXT',
                                                                       'YSG', 'YSX', 'DBLTME']:
                                work_minutes = int(
                                    timecards_json_parse.extract_minutes_from_timecards(daily_total['timeDuration']))
                            else:
                                work_minutes = 0
                            if daily_total['payCode']['codeValue'] in ['REGULAR', 'REGSAL', 'OVERTIME', 'MKU', 'EXT',
                                                                       'YSG', 'YSX', 'DBLTME'] or \
                                    daily_total['payCode']['codeValue'] in ['PTO', 'ETO-1', 'FLOAT', 'HOLIDAY', 'JURY',
                                                                            'CME', 'BEREAV', 'CED SAL', 'ESL', 'RLL',
                                                                            'SICK', 'CED']:
                                paid_minutes = int(
                                    timecards_json_parse.extract_minutes_from_timecards(daily_total['timeDuration']))
                            elif daily_total['payCode']['codeValue'] in ['UNPAID', 'PDLCHAWD', 'MEALPENALTY']:
                                # MEALPENALTY = meal penalty
                                # YSG = y surge non exempt - hourly
                                # YSX =  y surge exempt - salary
                                # CED= continuous education
                                # DBLTME = more generous rate than overtime, counts as actively worked time
                                # UNPAID = unpaid time off
                                # PDLCHAWD = unsure
                                paid_minutes = 0
                            else:
                                log_dictionary['failure_step'] = 'Unmapped paycode: ' + daily_total['payCode'][
                                    'codeValue']
                                paid_minutes = 0
                            timecard_list.append({
                                'employee_id': employee_id,
                                'job_title_id': job_title_id,
                                'period_start': period_start,
                                'date': entry_date,
                                'work_minutes': work_minutes,
                                'paid_minutes': paid_minutes,
                                'regular_minutes': regular_minutes
                            })
                        # else:
                        #     print(f'Failed querying timecard for {employee_id}')
                if len(timecard_list) != 0:
                    timecard_df = pd.DataFrame(timecard_list)
                    aggregated_df = timecard_df.groupby(
                        by=['employee_id', 'job_title_id', 'period_start', 'date']).sum().reset_index()
                    final_df = pd.concat([final_df, aggregated_df], ignore_index=True, axis=0)
            else:
                attempt += 1
                if attempt < 4:
                    time.sleep(.05)
                # else:
                #     print(f'Failed querying timecard for {employee_id}')

    final_df.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_timecards', if_exists='replace',
                    index=False)
    print('Success querying previous and current timecards and pushing to adp_timecards table: ' + str(
        final_df.shape[0]) + ' entries')
    log_dictionary['query_count'] = int(final_df.shape[0])
    api_connection.kill_connection()
except:
    error = 'Failure querying previous and current timecards and pushing to adp_timecards table'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    api_connection.kill_connection()
    prefect_email(error)
    sys.exit()

try:
    # insert new timecard dates into timecard_dates table
    insert_new_timecard_dates = """
    INSERT INTO workforce.dbo.timecard_dates
    SELECT DISTINCT
      period_start,
      date
    FROM workforce.dbo.adp_timecards
    EXCEPT
    SELECT
      period_start,
      date
    FROM workforce.dbo.timecard_dates
    ORDER BY period_start, date;
    """
    # update changed timecards and insert new timecards
    timecards_with_updates = """
    SELECT
      COUNT(*) AS updated_entry_counts
    FROM workforce.dbo.adp_timecards
      INNER JOIN workforce.dbo.employee_hours ON adp_timecards.employee_id = employee_hours.employee_id
      AND adp_timecards.date = employee_hours.date
    WHERE adp_timecards.work_minutes != employee_hours.work_minutes OR
          adp_timecards.paid_minutes != employee_hours.paid_minutes OR
          adp_timecards.regular_minutes != employee_hours.regular_minutes;"""

    updated_timecards_count = int(pd.read_sql(sql=timecards_with_updates, con=workforce_db_engine).iloc[0, 0])

    update_timecards = """
    UPDATE workforce.dbo.employee_hours
    SET work_minutes = adp_timecards.work_minutes,
        paid_minutes = adp_timecards.paid_minutes,
        regular_minutes = adp_timecards.regular_minutes
    FROM workforce.dbo.adp_timecards
    WHERE adp_timecards.employee_id = employee_hours.employee_id AND
          adp_timecards.date = employee_hours.date;"""

    # to match job titles between employee job history and employee hours due to job title realignment
    update_timecards_with_job_title_changes = """
    UPDATE workforce.dbo.employee_hours
    SET job_title_id = employee_job_history.job_title_id
    FROM workforce.dbo.employee_job_history
    WHERE employee_job_history.employee_id = employee_hours.employee_id AND
          employee_hours.date BETWEEN employee_job_history.start_date AND
            COALESCE(employee_job_history.end_date, '2099-12-31') AND
          employee_hours.job_title_id != employee_job_history.job_title_id;
          """

    new_timecards = """
    SELECT
      COUNT(*) AS new_entry_counts
    FROM workforce.dbo.adp_timecards
      LEFT JOIN workforce.dbo.employee_hours
                ON adp_timecards.employee_id = employee_hours.employee_id
                  AND adp_timecards.date = employee_hours.date
    WHERE employee_hours.employee_id IS NULL;"""

    new_timecards_count = int(pd.read_sql(sql=new_timecards, con=workforce_db_engine).iloc[0, 0])

    insert_timecards = """
    INSERT INTO workforce.dbo.employee_hours (employee_id, job_title_id, date, work_minutes, paid_minutes,regular_minutes)
    SELECT
      adp_timecards.employee_id,
      adp_timecards.job_title_id,
      adp_timecards.date,
      adp_timecards.work_minutes,
      adp_timecards.paid_minutes,
      adp_timecards.regular_minutes
    FROM workforce.dbo.adp_timecards
      LEFT JOIN workforce.dbo.employee_hours
                ON adp_timecards.employee_id = employee_hours.employee_id
                  AND adp_timecards.date = employee_hours.date
    WHERE employee_hours.employee_id IS NULL;"""

    drop_adp_timecards = """DROP TABLE IF EXISTS workforce.dbo.adp_timecards;"""

    with workforce_db_engine.connect() as connection:
        with connection.begin():
            connection.execute(text(insert_new_timecard_dates))
            connection.execute(text(update_timecards))
            connection.execute(text(update_timecards_with_job_title_changes))
            connection.execute(text(insert_timecards))
            connection.execute(text(drop_adp_timecards))

    log_dictionary['update_count'] = updated_timecards_count
    print(f'Updated employee_hours: {updated_timecards_count}')
    log_dictionary['insert_count'] = new_timecards_count
    print(f'Inserted employee_hours: {new_timecards_count}')
    print('Successfully updated Employee Hours table')
    log_dictionary['is_success'] = True
except:
    error = 'Failure updating and inserting into employee_hours'
    print(error)
    log_dictionary['failure_step'] = str(error)
    prefect_email(error)
finally:
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='adp_api_log', if_exists='append', index=False)
    workforce_db_engine.dispose()
    sys.exit()
