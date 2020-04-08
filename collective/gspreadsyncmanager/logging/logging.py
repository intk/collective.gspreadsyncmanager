# -*- coding: utf-8 -*-

from datetime import datetime
print_warnings = False
print_errors = True
print_status = True

def log_csv(message, err, timestamp):
    """
    Log format:
    datetime, type_error, message, exception
    """

    """if '[Error]' in message:
        pass
    elif '[Warning]' in message:
        pass
    elif '[Status]' in message:
        pass"""

    return ""

def log_sentry(message, err, timestamp):
    return ""

def logger(message, err=""):
    timestamp = datetime.today().isoformat()
    log_csv(message, err, timestamp)
    log_sentry(message, err, timestamp)

    print_message = False
    if '[Error]' in message and print_errors:
        print_message = True

    elif '[Status]' in message and print_status:
        print_message = True

    elif '[Warning]' in message and print_warnings:
        print_message = True
    else:
        print_message = False

    if print_message:
        if '[Status]' not in message:
            print("[%s] %s Exception: %s" %(timestamp, message, err))
        else:
            print("[%s] %s" %(timestamp, message))
    else:
        pass
