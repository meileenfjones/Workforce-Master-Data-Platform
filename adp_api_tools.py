import re


def get_id(worker):
    try:
        return int(next(work_assign['payrollFileNumber'] for work_assign in worker['workAssignments'] if
                        work_assign['primaryIndicator']))
    except:
        raise Exception('error getting id')


def get_aoid(worker):
    try:
        return worker['associateOID']
    except:
        raise Exception('error getting aoid')


def get_location_id(worker):
    try:
        return int(next(
            work_assign['homeWorkLocation']['nameCode']['codeValue'] for work_assign in worker['workAssignments'] if
            work_assign['primaryIndicator']))
    except KeyError:
        return 0
    except IndexError:
        return 0
    except:
        raise Exception('error getting location_id')


def get_location(worker):
    try:
        return next(
            work_assign['homeWorkLocation']['nameCode']['shortName'] for work_assign in worker['workAssignments'] if
            work_assign['primaryIndicator'])
    except KeyError:
        return 'Unassigned'
    except IndexError:
        return 'Unassigned'
    except:
        raise Exception('error getting location')


def get_department_id(worker):
    try:
        return int(next(work_assign['assignedOrganizationalUnits'][1]['nameCode']['codeValue'] for work_assign in
                        worker['workAssignments'] if work_assign['primaryIndicator']))
    except KeyError:
        return 0
    except IndexError:
        return 0
    except:
        raise Exception('error getting department_id')


def get_department(worker):
    try:
        try:
            return re.sub('\d+ - (.*)', r'\1',
                          next(work_assign['assignedOrganizationalUnits'][1]['nameCode']['shortName'] for work_assign in
                               worker['workAssignments'] if work_assign['primaryIndicator']))
        except:
            return re.sub('\d+ - (.*)', r'\1',
                          next(work_assign['assignedOrganizationalUnits'][1]['nameCode']['longName'] for work_assign in
                               worker['workAssignments'] if work_assign['primaryIndicator']))
    except KeyError:
        return 'Unassigned'
    except IndexError:
        return 'Unassigned'
    except:
        raise Exception('error getting department')


def get_reports_to_id(worker):
    try:
        return int(re.search('S3600(\d+)', next(
            work_assign['reportsTo'][0]['positionID'] for work_assign in worker['workAssignments'] if
            work_assign['primaryIndicator'])).group(1))
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting reports_to_id')


def get_job_title_id(worker):
    try:
        return int(next(work_assign['jobCode']['codeValue'] for work_assign in worker['workAssignments'] if
                        work_assign['primaryIndicator']))
    except:
        raise Exception('error getting job_title_id')


def get_job_title(worker):
    try:
        try:
            return next(work_assign['jobCode']['longName'] for work_assign in worker['workAssignments'] if
                        work_assign['primaryIndicator'])
        except:
            return next(work_assign['jobCode']['shortName'] for work_assign in worker['workAssignments'] if
                        work_assign['primaryIndicator'])
    except:
        raise Exception('error getting job_title')


def get_legal_first_name(worker):
    try:
        return worker['person']['legalName']['givenName']
    except:
        raise Exception('error getting legal_first_name')


def get_legal_middle_name(worker):
    try:
        return worker['person']['legalName']['middleName']
    except KeyError:
        return None
    except:
        raise Exception('error getting legal_middle_name')


def get_legal_last_name(worker):
    try:
        return worker['person']['legalName']['familyName1']
    except:
        raise Exception('error getting legal_last_name')


def get_preferred_first_name(worker):
    try:
        return worker['person']['preferredName']['givenName']
    except KeyError:
        return None
    except:
        raise Exception('error getting preferred_first_name')


def get_preferred_middle_name(worker):
    try:
        return worker['person']['preferredName']['middleName']
    except KeyError:
        return None
    except:
        raise Exception('error getting preferred_middle_name')


def get_preferred_last_name(worker):
    try:
        return worker['person']['preferredName']['familyName1']
    except KeyError:
        return None
    except:
        raise Exception('error getting preferred_last_name')


def get_gender(worker):
    try:
        return worker['person']['genderCode']['longName']
    except KeyError:
        return None
    except:
        raise Exception('error getting gender')


def get_pronouns(worker):
    try:
        return worker['person']['preferredGenderPronounCode']['shortName']
    except KeyError:
        return None
    except:
        raise Exception('error getting pronouns')


def get_work_email(worker):
    try:
        return worker['businessCommunication']['emails'][0]['emailUri'].lower()
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting work_email')


def get_personal_email(worker):
    try:
        return worker['person']['communication']['emails'][0]['emailUri'].lower()
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting personal_email')


def get_personal_mobile(worker):
    try:
        return worker['person']['communication']['mobiles'][0]['formattedNumber']
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting personal_mobile')


def get_status(worker):
    try:
        return worker['workerStatus']['statusCode']['codeValue']
    except KeyError:
        return None
    except:
        raise Exception('error getting status')


def get_employment_type(worker):
    try:
        try:
            return next(work_assign['workerTypeCode']['longName'] for work_assign in worker['workAssignments'] if
                        work_assign['primaryIndicator'])
        except:
            return next(work_assign['workerTypeCode']['shortName'] for work_assign in worker['workAssignments'] if
                        work_assign['primaryIndicator'])
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting employment_type')


def get_actual_seniority_date(worker):
    try:
        return (worker['customFieldGroup']['dateFields'][2])['dateValue']
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting actual_seniority_date')


def get_seniority_date(worker):
    try:
        return next(work_assign['seniorityDate'] for work_assign in worker['workAssignments'] if
                    work_assign['primaryIndicator'])
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting seniority_date')


def get_original_hire_date(worker):
    try:
        return worker['workerDates']['originalHireDate']
    except KeyError:
        return None
    except:
        raise Exception('error getting original_hire_date')


def get_hire_date(worker):
    try:
        return next(
            work_assign['hireDate'] for work_assign in worker['workAssignments'] if work_assign['primaryIndicator'])
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting hire_date')


def get_ultimate_hire_date(worker):
    if get_hire_date(worker) == '2023-10-01':
        if get_actual_seniority_date(worker) is not None:
            return get_actual_seniority_date(worker)
        elif get_seniority_date(worker) is not None:
            return get_seniority_date(worker)
        elif get_original_hire_date(worker) is not None:
            return get_original_hire_date(worker)
    else:
        return get_hire_date(worker)


def get_start_date(worker):
    try:
        return next(work_assign['actualStartDate'] for work_assign in worker['workAssignments'] if
                    work_assign['primaryIndicator'])
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting start_date')


def get_evaluation_date(worker):
    try:
        return (worker['customFieldGroup']['dateFields'][1])['dateValue']
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting evaluation_date')


def get_termination_date(worker):
    try:
        return worker['workerDates']['terminationDate']
    except KeyError:
        return None
    except:
        raise Exception('error getting termination_date')


def get_hired_fte(worker):
    try:
        return next(work_assign['fullTimeEquivalenceRatio'] for work_assign in worker['workAssignments'] if
                    work_assign['primaryIndicator'])
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting hired_fte')


def get_is_manager(worker):
    try:
        return next(work_assign['managementPositionIndicator'] for work_assign in worker['workAssignments'] if
                    work_assign['primaryIndicator'])
    except KeyError:
        return None
    except IndexError:
        return None
    except:
        raise Exception('error getting is_manager')


def extract_minutes_from_timecards(time_string):
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?'  # PT8H12M
    match = re.match(pattern, time_string)
    if match:
        hours = int(match.group(1) or '0')
        minutes = int(match.group(2) or '0')
        total_minutes = hours * 60 + minutes
        return total_minutes
    else:
        print(f'Possible Error in Format: {time_string}')
        return 0


def get_dob(worker):
    try:
        return worker['person']['birthDate']
    except:
        raise Exception('error getting dob')


def get_address_line_1(worker):
    try:
        return worker['person']['legalAddress']['lineOne']
    except:
        raise Exception('error getting address_line_1')


def get_address_line_2(worker):
    try:
        return worker['person']['legalAddress']['lineTwo']
    except KeyError:
        return None
    except:
        raise Exception('error getting address_line_2')


def get_city(worker):
    try:
        return worker['person']['legalAddress']['cityName']
    except:
        raise Exception('error getting city')


def get_state(worker):
    try:
        return worker['person']['legalAddress']['countrySubdivisionLevel1']['codeValue']
    except:
        raise Exception('error getting state')


def get_zip_code(worker):
    try:
        return worker['person']['legalAddress']['postalCode'][:5]
    except:
        raise Exception('error getting zip_code')
