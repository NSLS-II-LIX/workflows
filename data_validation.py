from prefect import task, flow, get_run_logger
import time as ttime
from tiled.client import from_profile

tiled_client = from_profile("nsls2")

@task(retries=2, retry_delay_seconds=10)
def read_all_streams(beamline_acronym, uid):
    logger = get_run_logger()
    run = tiled_client[beamline_acronym]["raw"][uid]
    logger.info(f"Validating uid {run.start['uid']}")
    start_time = ttime.monotonic()
    for stream in run:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")

@flow
def general_data_validation(beamline_acronym, uid):
    read_all_streams(beamline_acronym, uid)
