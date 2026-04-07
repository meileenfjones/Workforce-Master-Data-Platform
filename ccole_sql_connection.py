from pykeepass import PyKeePass
from urllib.parse import quote_plus
from sqlalchemy import create_engine


########################################################################
def SQLconnection(keepass_filename, keepass_keyfile, return_engine=False):
    """
    creates a connection or engine to the SQL SERVER database
    :param keepass_filename: keepass file path
    :param keepass_keyfile: keepass key file path
    :param return_engine: default false, boolean
    :return: if return_engine=True, returns engine; else, returns connection
    """
    # uses credentials to connect to database
    keepass_db = PyKeePass(filename=keepass_filename, keyfile=keepass_keyfile)
    database = keepass_db.find_entries(title='SQLSERVER', first=True)
    db_driver = 'driver=' + database.custom_properties['driver'] + ';'
    db_server = 'server=' + database.custom_properties['server'] + ';'
    # db_name = 'database=' + database.custom_properties['database'] + ';'
    db_name = 'database=' + 'workforce' + ';'
    db_username = 'uid=' + database.username + ';'
    db_password = 'pwd=' + database.password + ';'
    db_connection_string = quote_plus(db_driver + db_server + db_name + db_username + db_password)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(db_connection_string), fast_executemany=True)
    if return_engine:
        return engine
    else:
        return engine.connect()
