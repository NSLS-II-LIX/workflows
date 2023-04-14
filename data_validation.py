from datetime import timedelta
import prefect
from prefect import task, Flow, Parameter
import time as ttime
from tiled.client import from_profile


@task(max_retries=2, retry_delay=timedelta(seconds=10))
def read_all_streams(beamline_acronym, uid):
    logger = prefect.context.get("logger")
    c = from_profile("nsls2", username=None)
    try:
        run = c[beamline_acronym][uid]
    except KeyError:
        run = c[beamline_acronym]["raw"][uid]
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


with Flow("general-data-validation") as flow:
    beamline_acronym = Parameter("beamline_acronym")
    uid = Parameter("uid")
    read_all_streams(beamline_acronym, uid)
