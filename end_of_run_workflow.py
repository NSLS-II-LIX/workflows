import prefect
from prefect import flow, task, get_run_logger
from data_validation import general_data_validation

@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")

@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]

    general_data_validation(beamline_acronym="lix", uid=uid)
    log_completion()
