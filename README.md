# Workforce

__Background__  
Workforce is a project to maintain a list of staff for reporting and internal tracking purposes.

__Deliverables__  
The final product will be tables that support data for Network Users, Employees, Learners, Contractors, and Volunteers

## Prerequisites

List all items needed for the program to work

- Windows OS
- Task Scheduler
- Secure credential manager (e.g., KeePass)
    - API or database credentials stored securely
- Python (3.11.3)
  - SQLAlchemy >= 2.0.34 
  - pandas >= 2.2.2 
  - pykeepass >= 4.1.0.post1 
  - pyodbc >= 5.1.0 
  - ldap3>=2.9.1 
  - psycopg2>=2.9.6 
  - prefect==3.1.0

### Setup

List all packages/technologies needed to build the program

1. Download most recent python on C drive [latest version for Windows](https://www.python.org/downloads/)
2. Download the [latest release](https://github.com/OleHealth/Crossroads-Survey-Automation/releases/latest) and unzip.
3. Open a Windows Command Prompt Terminal
4. Change directory to the script folder

  ```
  cd Workforce
  ```

5. Create virtual environment

  ```
  python -m venv venv
  ```

6. Activate the virtual environment

  ```
  venv\Scripts\activate.bat
  ```

7. Install Python packages

  ```
  pip install -r requirements.txt
  ```

8. Run Necessary Commands for Prefect

  ```
  pip install "prefect[email]"
  prefect cloud login -k "block key"
  prefect block register -m prefect_email

  ```

9. Create config.py and fill out with following paths

```
from pathlib import Path
############################################################################################################
# specified keepass file and key path for this project

KDBX_FILE = 
KEY_PATH = 

############################################################################################################

```

10. Create/verify database schema and tables as well as logging table

11. To automate project, create tasks with Windows Task Scheduler directed at the bat files set to run at specified times


## Documentation


### Files

List important files to know about and how they work together

`config.py`
- Contains paths for KeePass file and key path

`ddl`
- Folder containing sql queries used to create Workforce tables, views, logs, and audit queries

`adp_api_connection.py`
- connect to ADP API for querying workers and timecards

`adp_api_tools.py`
- tools to parse/clean specific info from ADP

`active_directory_tools.py`
- tools to parse/clean/update specific info in Active Directory

`ccole_sql_connection.py`
- connect to internal server and workforce database

`relevant_connection.py`
- connect to analytics server

`active_directory_users.py`
- script to query and update network_users table

`adp_workers.py`
- script to query Workers and update Employee related tables

`adp_timecards.py`
- script to query Timecards and update Employee Hours related tables

`workforce_map_updates.py`
- script to insert new matches between Employees and Network Users into workforce_map and update existing rows with 
epic id and relevant id

### Methodology

1. Connect to ADP API and internal data source
2. Query Workers (5:30 AM) and Timecards (5:00 AM Sun, Th) 
3. Update Employee tables with updated or inserted data
4. Logs the script run in log
5. Connect to Active Directory to query and update Network Users table (5:35 AM) 
6. Update workforce_map table with changes and new matches (5:45 AM) 
7. Acquisition plan pulls workforce data into analytics platform (5:55 AM M-F)


## Resources

List resources used from the internet or helpful links
- Project List: https://api-central.adp.com/projects
- API Explorer: https://developers.adp.com/apis/api-explorer/hcm-offrg-wfn
- Data Dictionary: https://developers.adp.com/guides/api-guides/worker-management-api-guide-for-adp-workforce-now/chapter/11


