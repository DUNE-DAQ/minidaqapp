from glob import glob

def log_has_no_errors(log_file_name):
    ok=True
    for line in open(log_file_name).readlines():
        # The first two words in the line are the date and time, then the severity
        severity=line.split()[2]
        if severity in ("WARN", "ERROR", "FATAL"):
            print(line)
            ok=False
    return ok

def logs_are_error_free():
    all_ok=True
    for log in glob("log_*.txt"):
        all_ok=all_ok and log_has_no_errors(log)
    return all_ok
