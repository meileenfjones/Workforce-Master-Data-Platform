import re


##################################################################################################


def get_primary_email(proxy_addresses):
    regex = r'SMTP:(.+)'
    if proxy_addresses is None:
        return None
    else:
        aliases = [re.search(regex, email).group(1) for email in proxy_addresses if re.search(regex, email)]
        return aliases[0].lower() if len(aliases) > 0 else None


email_duplicates = """
WITH dupe_priority AS
       (
         SELECT
           id,
           guid,
           network_users.email,
           is_deleted,
           is_disabled,
           IIF(network_users.email LIKE '%organizationname.org%', 1, 0) AS email_flag,
           created_at,
           inserted_at,
           ROW_NUMBER()
             OVER (PARTITION BY network_users.email ORDER BY is_deleted, is_disabled, IIF(network_users.email LIKE '%organizationname.org%', 1, 0) DESC, created_at DESC, inserted_at DESC) AS row_priority
         FROM workforce.dbo.network_users
           INNER JOIN (
           SELECT
             email
           FROM workforce.dbo.network_users
           WHERE email IS NOT NULL AND
                 is_non_user = 0 AND
                 is_primary_acct = 1
           GROUP BY email
           HAVING COUNT(*) > 1
         ) AS email_dupes ON network_users.email = email_dupes.email
         WHERE network_users.email IS NOT NULL AND
               is_non_user = 0
       ),
     final_updates AS
       (
         SELECT
           primary_acct.guid AS primary_guid,
           secondary_acct.id,
           secondary_acct.email
         FROM dupe_priority primary_acct
           INNER JOIN dupe_priority AS secondary_acct ON primary_acct.email = secondary_acct.email
         WHERE primary_acct.row_priority = 1 AND
               secondary_acct.row_priority > 1
       )
UPDATE workforce.dbo.network_users
SET is_primary_acct = 0,
    primary_guid    = final_updates.primary_guid
FROM final_updates
WHERE network_users.id = final_updates.id;"""

primary_email_duplicates = """
WITH dupe_priority AS
       (
         SELECT
           id,
           guid,
           network_users.primary_email,
           is_deleted,
           is_disabled,
           IIF(network_users.primary_email LIKE '%organizationname.org%', 1, 0) AS email_flag,
           created_at,
           inserted_at,
           ROW_NUMBER()
             OVER (PARTITION BY network_users.primary_email ORDER BY is_deleted, is_disabled, IIF(network_users.primary_email LIKE '%organizationname.org%', 1, 0) DESC, created_at DESC, inserted_at DESC) AS row_priority
         FROM workforce.dbo.network_users
           INNER JOIN (
           SELECT
             primary_email
           FROM workforce.dbo.network_users
           WHERE primary_email IS NOT NULL AND
                 is_non_user = 0 AND
                 is_primary_acct = 1
           GROUP BY primary_email
           HAVING COUNT(*) > 1
         ) AS primary_email_dupes ON network_users.primary_email = primary_email_dupes.primary_email
         WHERE network_users.primary_email IS NOT NULL AND
               is_non_user = 0
       ),
     final_updates AS
       (
         SELECT
           primary_acct.guid AS primary_guid,
           secondary_acct.id,
           secondary_acct.primary_email
         FROM dupe_priority primary_acct
           INNER JOIN dupe_priority AS secondary_acct ON primary_acct.primary_email = secondary_acct.primary_email
         WHERE primary_acct.row_priority = 1 AND
               secondary_acct.row_priority > 1
       )
UPDATE workforce.dbo.network_users
SET is_primary_acct = 0,
    primary_guid    = final_updates.primary_guid
FROM final_updates
WHERE network_users.id = final_updates.id;"""

network_email_duplicates = """
WITH dupe_priority AS
       (
         SELECT
           id,
           guid,
           network_users.network_email,
           is_deleted,
           is_disabled,
           IIF(network_users.network_email LIKE '%organizationname.org%', 1, 0) AS email_flag,
           created_at,
           inserted_at,
           ROW_NUMBER()
             OVER (PARTITION BY network_users.network_email ORDER BY is_deleted, is_disabled, IIF(network_users.network_email LIKE '%organizationname.org%', 1, 0) DESC, created_at DESC, inserted_at DESC) AS row_priority
         FROM workforce.dbo.network_users
           INNER JOIN (
           SELECT
             network_email
           FROM workforce.dbo.network_users
           WHERE network_email IS NOT NULL AND
                 is_non_user = 0 AND
                 is_primary_acct = 1
           GROUP BY network_email
           HAVING COUNT(*) > 1
         ) AS network_email_dupes ON network_users.network_email = network_email_dupes.network_email
         WHERE network_users.network_email IS NOT NULL AND
               is_non_user = 0
       ),
     final_updates AS
       (
         SELECT
           primary_acct.guid AS primary_guid,
           secondary_acct.id,
           secondary_acct.network_email
         FROM dupe_priority primary_acct
           INNER JOIN dupe_priority AS secondary_acct ON primary_acct.network_email = secondary_acct.network_email
         WHERE primary_acct.row_priority = 1 AND
               secondary_acct.row_priority > 1
       )
UPDATE workforce.dbo.network_users
SET is_primary_acct = 0,
    primary_guid    = final_updates.primary_guid
FROM final_updates
WHERE network_users.id = final_updates.id;"""

common_name_duplicates = """
WITH dupe_priority AS
       (
         SELECT
           id,
           guid,
           network_users.common_name,
           is_deleted,
           is_disabled,
           created_at,
           inserted_at,
           ROW_NUMBER()
             OVER (PARTITION BY network_users.common_name ORDER BY is_deleted, is_disabled, created_at DESC, inserted_at DESC) AS row_priority
         FROM workforce.dbo.network_users
           INNER JOIN (
           SELECT
             common_name
           FROM workforce.dbo.network_users
           WHERE common_name IS NOT NULL AND
                 is_non_user = 0 AND
                 is_primary_acct = 1
           GROUP BY common_name
           HAVING COUNT(*) > 1
         ) AS common_name_dupes ON network_users.common_name = common_name_dupes.common_name
         WHERE network_users.common_name IS NOT NULL AND
               is_non_user = 0
       ),
     final_updates AS
       (
         SELECT
           primary_acct.guid AS primary_guid,
           secondary_acct.id,
           secondary_acct.common_name
         FROM dupe_priority primary_acct
           INNER JOIN dupe_priority AS secondary_acct ON primary_acct.common_name = secondary_acct.common_name
         WHERE primary_acct.row_priority = 1 AND
               secondary_acct.row_priority > 1
       )
UPDATE workforce.dbo.network_users
SET is_primary_acct = 0,
    primary_guid    = final_updates.primary_guid
FROM final_updates
WHERE network_users.id = final_updates.id;"""

display_name_duplicates = """
WITH dupe_priority AS
       (
         SELECT
           id,
           guid,
           network_users.display_name,
           is_deleted,
           is_disabled,
           created_at,
           inserted_at,
           ROW_NUMBER()
             OVER (PARTITION BY network_users.display_name ORDER BY is_deleted, is_disabled, created_at DESC, inserted_at DESC) AS row_priority
         FROM workforce.dbo.network_users
           INNER JOIN (
           SELECT
             display_name
           FROM workforce.dbo.network_users
           WHERE display_name IS NOT NULL AND
                 is_non_user = 0 AND
                 is_primary_acct = 1
           GROUP BY display_name
           HAVING COUNT(*) > 1
         ) AS display_name_dupes ON network_users.display_name = display_name_dupes.display_name
         WHERE network_users.display_name IS NOT NULL AND
               is_non_user = 0
       ),
     final_updates AS
       (
         SELECT
           primary_acct.guid AS primary_guid,
           secondary_acct.id,
           secondary_acct.display_name
         FROM dupe_priority primary_acct
           INNER JOIN dupe_priority AS secondary_acct ON primary_acct.display_name = secondary_acct.display_name
         WHERE primary_acct.row_priority = 1 AND
               secondary_acct.row_priority > 1
       )
UPDATE workforce.dbo.network_users
SET is_primary_acct = 0,
    primary_guid    = final_updates.primary_guid
FROM final_updates
WHERE network_users.id = final_updates.id;"""

concatenated_name_duplicates = """
WITH dupe_priority AS
       (
         SELECT
           id,
           guid,
           CONCAT_WS(' ', first_name, last_name) AS concatenated_name,
           is_deleted,
           is_disabled,
           created_at,
           inserted_at,
           ROW_NUMBER()
             OVER (PARTITION BY CONCAT_WS(' ', network_users.first_name, network_users.last_name) ORDER BY is_deleted, is_disabled, created_at DESC, inserted_at DESC) AS row_priority
         FROM workforce.dbo.network_users
           INNER JOIN (
           SELECT
             CONCAT_WS(' ', first_name, last_name) AS concatenated_name
           FROM workforce.dbo.network_users
           WHERE CONCAT_WS(' ', first_name, last_name) != '' AND
                 is_non_user = 0 AND
                 is_primary_acct = 1
           GROUP BY CONCAT_WS(' ', first_name, last_name)
           HAVING COUNT(*) > 1
         ) AS concatenated_name_dupes
                      ON CONCAT_WS(' ', first_name, last_name) = concatenated_name_dupes.concatenated_name
         WHERE CONCAT_WS(' ', first_name, last_name) != '' AND
               is_non_user = 0
       ),
     final_updates AS
       (
         SELECT
           primary_acct.guid AS primary_guid,
           secondary_acct.id,
           secondary_acct.concatenated_name
         FROM dupe_priority primary_acct
           INNER JOIN dupe_priority AS secondary_acct
                      ON primary_acct.concatenated_name = secondary_acct.concatenated_name
         WHERE primary_acct.row_priority = 1 AND
               secondary_acct.row_priority > 1
       )
UPDATE workforce.dbo.network_users
SET is_primary_acct = 0,
    primary_guid    = final_updates.primary_guid
FROM final_updates
WHERE network_users.id = final_updates.id;"""
