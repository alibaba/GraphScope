# Those methods should not be implemented in AdminService, but is still kept here, cause the python flask app relies on the openpai_interactive.yaml to launch the service, which needs these function definitions.
# To run_adhoc_current/call_proc_current/call_proc/run_adhoc, send requests to query service.
def run_adhoc_current():
    raise NotImplementedError("run_adhoc_current is not implemented in admin service, please send to query service")


def call_proc_current():
    raise NotImplementedError("call_proc_current is not implemented in admin service, please send to query service")


def call_proc():
    raise NotImplementedError("call_proc is not implemented in admin service, please send to query service")


def run_adhoc():
    raise NotImplementedError("run_adhoc is not implemented in admin service, please send to query service")
