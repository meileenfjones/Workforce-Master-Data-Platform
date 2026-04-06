from ldap3 import Connection, SUBTREE, NTLM
import pandas as pd
import re
from pykeepass import PyKeePass
import sys
import config
from sqlalchemy.sql import text
from sql_connection import SQLconnection
from active_directory_tools import get_primary_email
import active_directory_tools as clean_up_scripts
from prefect import flow
from prefect_email import EmailServerCredentials, email_send_message


######################################################################################################################

@flow
def prefect_email(error_message):
    email_server_credentials = EmailServerCredentials.load("prefect-ddbot-email")
    email_addresses = [email_to_notify]
    print(error_message)
    for email_address in email_addresses:
        email_send_message.with_options(name=f"email {email_address}").submit(
            email_server_credentials=email_server_credentials,
            subject="Workforce - Active Directory Users Script Error",
            msg=error_message,
            email_to=email_address,
        )


try:
    log = {}
    workforce_db_engine = SQLconnection(keepass_filename=config.KDBX_FILE, keepass_keyfile=config.KEY_PATH,
                                             return_engine=True)
    # connect to KeePass database
    keepass_db = PyKeePass(filename=config.KDBX_FILE, keyfile=config.KEY_PATH)
    keepass_entry = keepass_db.find_entries(title='Apps', first=True)
    app_user = 'SERVER\\' + keepass_entry.username
    app_password = keepass_entry.password

    # connect to server 1 active directory
    server1_conn = Connection(server='SERVER1.local', user=app_user, password=app_password,
                          read_only=True, authentication=NTLM)
    server1_conn.bind()

    # query results and save to dictionary
    base_dn = 'OU=Users,OU=SERVER1,DC=SERVER1,DC=local'
    attributes = ['objectGUID', 'givenName', 'sn', 'cn', 'displayName', 'userPrincipalName', 'proxyAddresses', 'mail',
                  'title', 'sAMAccountName', 'distinguishedName', 'department', 'physicalDeliveryOfficeName', 'manager',
                  'description', 'telephoneNumber', 'mobile', 'whenCreated', 'userAccountControl']
    filter = '(objectClass=User)'
    result = server1_conn.search(search_base=base_dn, search_filter=filter, attributes=attributes, search_scope=SUBTREE)
    if result:
        for staff in range(len(server1_conn.entries)):
            log[staff] = {
                'guid': server1_conn.entries[staff].objectGUID.value[1:-1] if server1_conn.entries[
                                                                              staff].objectGUID.value is not None else None,
                'source': 'server1',
                'first_name': server1_conn.entries[staff].givenName.value.strip().title() if server1_conn.entries[
                                                                                             staff].givenName.value is not None else None,
                'last_name': server1_conn.entries[staff].sn.value.strip().title() if server1_conn.entries[
                                                                                     staff].sn.value is not None else None,
                'common_name': server1_conn.entries[staff].cn.value.strip().title() if server1_conn.entries[
                                                                                       staff].cn.value is not None else None,
                'display_name': server1_conn.entries[staff].displayName.value.strip().title() if server1_conn.entries[
                                                                                                 staff].displayName.value is not None else None,
                'network_email': server1_conn.entries[staff].userPrincipalName.value.strip().lower() if server1_conn.entries[
                                                                                                        staff].userPrincipalName.value is not None else None,
                'primary_email': get_primary_email(server1_conn.entries[staff].proxyAddresses.value),
                'email': server1_conn.entries[staff].mail.value.strip().lower() if server1_conn.entries[
                                                                                   staff].mail.value is not None else None,
                'job_title': server1_conn.entries[staff].title.value.strip() if server1_conn.entries[
                                                                                staff].title.value is not None else None,
                'account_name': server1_conn.entries[staff].sAMAccountName.value.strip().lower() if server1_conn.entries[
                                                                                                    staff].sAMAccountName.value is not None else None,
                'active_dir_group': server1_conn.entries[staff].distinguishedName.value.split(',')[-5][3:],
                'department': server1_conn.entries[staff].department.value.strip() if server1_conn.entries[
                                                                                      staff].department.value is not None else None,
                'location': server1_conn.entries[staff].physicalDeliveryOfficeName.value.strip() if server1_conn.entries[
                                                                                                    staff].physicalDeliveryOfficeName.value is not None else None,
                'manager': re.search(r'CN=([^,]+)', server1_conn.entries[staff].manager.value).group(1) if server1_conn.entries[
                                                                                                           staff].manager.value is not None else None,
                'user_description': server1_conn.entries[staff].description.value,
                'phone_extension': server1_conn.entries[staff].telephoneNumber.value,
                'mobile_phone': server1_conn.entries[staff].mobile.value,
                'created_at': server1_conn.entries[staff].whenCreated.value.strftime('%Y-%m-%d %H:%M:%S'),
                # enabled = 512 & 66048, disabled = 514,66050
                'is_disabled': False if server1_conn.entries[
                                            staff].userAccountControl.value in [512, 66048] else True
            }
    print('Queried ' + str(len(server1_conn.entries)) + ' entries in SERVER1 Active Directory')

    # connect to SERVER2 active directory
    server2_conn = Connection(server='SERVER2.local', user=app_user, password=app_password,
                         read_only=True, authentication=NTLM)
    server2_conn.bind()
    search_bases = ['OU=Users - Active Human,DC=communicare,DC=local']
    for base_dn in search_bases:
        result = server2_conn.search(search_base=base_dn, search_filter=filter, attributes=attributes, search_scope=SUBTREE)
        if result:
            for staff in range(len(server2_conn.entries)):
                log[base_dn + str(staff)] = {
                    'guid': server2_conn.entries[staff].objectGUID.value[1:-1] if server2_conn.entries[
                                                                                 staff].objectGUID.value is not None else None,
                    'source': 'server2',
                    'first_name': server2_conn.entries[staff].givenName.value.strip().title() if server2_conn.entries[
                                                                                                staff].givenName.value is not None else None,
                    'last_name': server2_conn.entries[staff].sn.value.strip().title() if server2_conn.entries[
                                                                                        staff].sn.value is not None else None,
                    'common_name': server2_conn.entries[staff].cn.value.strip().title() if server2_conn.entries[
                                                                                          staff].cn.value is not None else None,
                    'display_name': server2_conn.entries[staff].displayName.value.strip().title() if server2_conn.entries[
                                                                                                    staff].displayName.value is not None else None,
                    'network_email': server2_conn.entries[staff].userPrincipalName.value.strip().lower() if server2_conn.entries[
                                                                                                           staff].userPrincipalName.value is not None else None,
                    'primary_email': get_primary_email(server2_conn.entries[staff].proxyAddresses.value),
                    'email': server2_conn.entries[staff].mail.value.strip().lower() if server2_conn.entries[
                                                                                      staff].mail.value is not None else None,
                    'job_title': server2_conn.entries[staff].title.value.strip() if server2_conn.entries[
                                                                                   staff].title.value is not None else None,
                    'account_name': server2_conn.entries[staff].sAMAccountName.value.strip().lower() if server2_conn.entries[
                                                                                                       staff].sAMAccountName.value is not None else None,
                    'active_dir_group': server2_conn.entries[staff].distinguishedName.value.split(',')[-4][3:],
                    'department': server2_conn.entries[staff].department.value.strip() if server2_conn.entries[
                                                                                         staff].department.value is not None else None,
                    'location': server2_conn.entries[staff].physicalDeliveryOfficeName.value.strip() if server2_conn.entries[
                                                                                                       staff].physicalDeliveryOfficeName.value is not None else None,
                    'manager': re.search(r'CN=([^,]+)', server2_conn.entries[staff].manager.value).group(1) if
                    server2_conn.entries[
                        staff].manager.value is not None else None,
                    'user_description': server2_conn.entries[staff].description.value,
                    'phone_extension': server2_conn.entries[staff].telephoneNumber.value,
                    'mobile_phone': server2_conn.entries[staff].mobile.value,
                    'created_at': server2_conn.entries[staff].whenCreated.value.strftime('%Y-%m-%d %H:%M:%S') if
                    server2_conn.entries[
                        staff].whenCreated.value is not None else None,
                    # enabled = 512 & 66048, disabled = 514,66050
                    'is_disabled': False if server2_conn.entries[
                                                staff].userAccountControl.value in [512, 66048] else True
                }
    print('Queried ' + str(len(server2_conn.entries)) + ' entries in SERVER2 Active Directory')
except:
    error = 'Failure in connecting/querying active directory'
    print(error)
    prefect_email(error)
    workforce_db_engine.dispose()
    sys.exit()

try:
    # Create table for all current active directory entries
    current_ad_roster = pd.DataFrame.from_dict(log, orient='index')
    print('Queried ' + str(current_ad_roster.shape[0]) + ' entries in Active Directory')
    current_ad_roster.to_sql(con=workforce_db_engine, schema='workforce.dbo', name='network_users_temp',
                             if_exists='replace', index=False)
    # Get differences between current AD roster and network_users
    ad_columns = """
      guid,
      source,
      first_name,
      last_name,
      common_name,
      display_name,
      network_email,
      primary_email,
      email,
      job_title,
      account_name,
      active_dir_group,
      department,
      location,
      manager,
      user_description,
      phone_extension,
      mobile_phone,
      created_at,
      is_disabled"""

    roster_change_query = f"""
    SELECT {ad_columns}
    FROM workforce.dbo.network_users_temp
    EXCEPT
    select {ad_columns}
    from workforce.dbo.network_users"""

    roster_changes = pd.DataFrame(pd.read_sql(sql=roster_change_query, con=workforce_db_engine))
    print('Number of roster changes: ' + str(roster_changes.shape[0]))
except:
    error = 'Failure in getting roster changes'
    print(error)
    prefect_email(error)
    workforce_db_engine.dispose()
    sys.exit()

try:
    # if there are no changes or additions, stop
    if roster_changes.shape[0] == 0:
        drop_active_directory_table = """DROP TABLE IF EXISTS workforce.dbo.network_users_temp;"""
        with workforce_db_engine.connect() as connection:
            with connection.begin():
                connection.execute(text(drop_active_directory_table))
        raise Exception
except:
    print('NO CHANGES')
    workforce_db_engine.dispose()
    sys.exit()

try:
    # Update and insert network_users with updated/new rows
    updated_staff_count_query = """
    SELECT
      COUNT(*) AS count
    FROM workforce.dbo.network_users
      INNER JOIN workforce.dbo.network_users_temp AS temp ON network_users.guid = temp.guid
    WHERE network_users.first_name != temp.first_name OR
          network_users.last_name != temp.last_name OR
          network_users.common_name != temp.common_name OR
          network_users.display_name != temp.display_name OR
          network_users.network_email != temp.network_email OR
          network_users.primary_email != temp.primary_email OR
          network_users.email != temp.email OR
          network_users.job_title != temp.job_title OR
          network_users.account_name != temp.account_name OR
          network_users.active_dir_group != temp.active_dir_group OR
          network_users.department != temp.department OR
          network_users.location != temp.location OR
          network_users.manager != temp.manager OR
          network_users.user_description != temp.user_description OR
          network_users.phone_extension != temp.phone_extension OR
          network_users.mobile_phone != temp.mobile_phone OR
          network_users.created_at != temp.created_at OR
          network_users.is_disabled != temp.is_disabled;
      """
    updated_staff_count = int(pd.read_sql(sql=updated_staff_count_query, con=workforce_db_engine).iloc[0, 0])

    update_staff_statement = f"""
    UPDATE workforce.dbo.network_users
    SET guid             = diff.guid,
        first_name       = diff.first_name,
        last_name        = diff.last_name,
        common_name      = diff.common_name,
        display_name     = diff.display_name,
        network_email    = diff.network_email,
        primary_email    = diff.primary_email,
        email            = diff.email,
        job_title        = diff.job_title,
        account_name     = diff.account_name,
        active_dir_group = diff.active_dir_group,
        department       = diff.department,
        location         = diff.location,
        manager          = diff.manager,
        user_description = diff.user_description,
        phone_extension  = diff.phone_extension,
        mobile_phone     = diff.mobile_phone,
        created_at       = diff.created_at,
        is_disabled      = diff.is_disabled,
        disabled_at      = IIF(network_users.is_disabled = 0 AND diff.is_disabled = 1, GETDATE(), NULL)
    FROM ({roster_change_query}) AS diff
    WHERE workforce.dbo.network_users.guid = diff.guid;
    """

    undeleted_staff_count_query = """
    SELECT
      COUNT(*) AS count
    FROM workforce.dbo.network_users
      INNER JOIN workforce.dbo.network_users_temp AS temp 
      ON network_users.guid = temp.guid
    WHERE network_users.is_deleted = 1;
    """

    undeleted_staff_count = int(pd.read_sql(sql=undeleted_staff_count_query, con=workforce_db_engine).iloc[0, 0])

    update_undeleted_staff_statement = f"""
    UPDATE workforce.dbo.network_users
    SET is_deleted = 0,
        deleted_at = NULL
    FROM workforce.dbo.network_users
      INNER JOIN workforce.dbo.network_users_temp AS temp 
      ON network_users.guid = temp.guid
    WHERE network_users.is_deleted = 1;
        """

    deleted_staff_count_query = """
    SELECT
      COUNT(*) AS count
    FROM workforce.dbo.network_users
      LEFT JOIN workforce.dbo.network_users_temp AS temp ON network_users.guid = temp.guid
    WHERE temp.guid IS NULL AND
          network_users.is_deleted = 0;
    """
    deleted_staff_count = int(pd.read_sql(sql=deleted_staff_count_query, con=workforce_db_engine).iloc[0, 0])

    update_deleted_staff_statement = f"""
    UPDATE workforce.dbo.network_users
    SET is_deleted = 1,
        deleted_at = GETDATE()
    FROM workforce.dbo.network_users
      LEFT JOIN workforce.dbo.network_users_temp AS temp
                ON network_users.guid = temp.guid
    WHERE temp.guid IS NULL AND
          network_users.is_deleted = 0;
        """

    insert_staff_count_query = f"""
    SELECT
      COUNT(*) AS count
    FROM ({roster_change_query}) AS diff
    WHERE diff.guid NOT IN (
      SELECT
        guid
      FROM workforce.dbo.network_users
    );
        """
    inserted_staff_count = int(pd.read_sql(sql=insert_staff_count_query, con=workforce_db_engine).iloc[0, 0])

    insert_new_staff_statement = f"""
    INSERT INTO workforce.dbo.network_users ({ad_columns}, primary_guid, disabled_at)
    SELECT {ad_columns}, guid, IIF(is_disabled = 1, GETDATE(), NULL)
    FROM ({roster_change_query}) as diff
    WHERE diff.guid NOT IN (SELECT guid FROM workforce.dbo.network_users);
         """

    drop_active_directory_table = """
    DROP TABLE IF EXISTS workforce.dbo.network_users_temp;
         """
    with workforce_db_engine.connect() as connection:
        with connection.begin():
            connection.execute(text(update_staff_statement))
            connection.execute(text(update_undeleted_staff_statement))
            connection.execute(text(update_deleted_staff_statement))
            connection.execute(text(insert_new_staff_statement))
            connection.execute(text(drop_active_directory_table))
    print(f'Updated staff: {updated_staff_count}')
    print(f'New deleted staff: {deleted_staff_count}')
    print(f'New staff: {inserted_staff_count}')

    with workforce_db_engine.connect() as connection:
        with connection.begin():
            connection.execute(text(clean_up_scripts.email_duplicates))
            connection.execute(text(clean_up_scripts.primary_email_duplicates))
            connection.execute(text(clean_up_scripts.network_email_duplicates))
            connection.execute(text(clean_up_scripts.common_name_duplicates))
    print('Successfully updated network_users table')
except:
    error = 'Failure pushing changes to network_users'
    print(error)
    prefect_email(error)
finally:
    workforce_db_engine.dispose()
    sys.exit()
