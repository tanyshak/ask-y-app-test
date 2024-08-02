from datetime import datetime

def validate_date_range(start_date_str, end_date_str, very_start_date_str, very_end_date_str):
    try:
        start_date = datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.strptime(end_date_str, '%Y%m%d')
        very_start_date = datetime.strptime(very_start_date_str, '%Y%m%d')
        very_end_date = datetime.strptime(very_end_date_str, '%Y%m%d')

        if start_date < very_start_date or end_date > very_end_date:
            return False
        if start_date > end_date:
            return False
        return True
    except ValueError:
        return False

import re

def validate_date_format(date_str):
    return re.match(r'^\d{8}$', date_str) is not None

def validate_account_format(account):
    """Validate Snowflake account format (e.g., 'account')."""
    return re.match(r'^[a-zA-Z0-9_-]+$', account) is not None

def validate_snowflake_form(user, password, account):
    """Validate the Snowflake form inputs."""
    errors = []
    if not user:
        errors.append("User is required.")
    if not password:
        errors.append("Password is required.")
    if not account:
        errors.append("Account is required.")
    elif not validate_account_format(account):
        errors.append("Account must only contain alphanumeric characters, dashes, and underscores.")
    return errors
