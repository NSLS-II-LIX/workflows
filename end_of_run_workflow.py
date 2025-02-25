import prefect
from prefect import flow, task, get_run_logger

@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")

@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]

    log_completion()
