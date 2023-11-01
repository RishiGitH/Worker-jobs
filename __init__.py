import traceback
from datetime import datetime
from arq import cron
from src.db import db
from src.db.crud import update_record, read_record
from src.worker.logger_config import worker_logger, configure_logging
from src.db.crud.jobs import set_job_schedule_to_na
from src.db.crud.job_history import update_job_hist_status, create_job_history
from src.db.crud.job_schedules import read_schedules_to_run, update_schedule_next_exec

from src.db.crud.jobs import read_jobs
from src.db.models import JobHistory, JobState
from src.db.enum import JobStatesEnum
from src.redis_queue.queue import Queue
from src.worker import sync_data
from src.worker.load_jobs import LoadJobs
from src.worker.trigger_he_apis import TriggerHeApis

JOB_MAPPING = {"sync_data": sync_data}


async def startup(_):
    configure_logging()
    worker_logger.info("Starting up")
    await db.connect()


async def shutdown(_):
    await db.disconnect()


async def run_job_post_conn(_, connection, user_guid):
    await LoadJobs().create_job_post_connection(connection, user_guid)


async def run_job_post_collage(_, collage):
    await LoadJobs().create_job_post_collage(collage)


async def run_job_create_he_conn(_, connection):
    await TriggerHeApis().trigger_he_create_connection(connection)


async def run_job_trigger_he_execution(_, job_history):
    await TriggerHeApis().trigger_he_job_execution(job_history)


async def run_job(_, job_hist: JobHistory):
    await update_job_hist_status(job_hist, JobStatesEnum.RUNNING.value)
    try:
        # TODO: hardcoded right now, maybe we should infer the function to run using the process
        # type or something else
        await JOB_MAPPING["sync_data"].run(job_hist)
        await update_job_hist_status(job_hist, JobStatesEnum.SUCCESS.value)
    # improve error handling
    # pragma pylint: disable=broad-except
    except Exception as err:
        await update_job_hist_status(
            job_hist,
            JobStatesEnum.ERROR.value,
            str(traceback.format_exc()),
        )
        await set_job_schedule_to_na(job_hist.job_fk)
        worker_logger.info(str(err))
        worker_logger.info(str(traceback.format_exc()))
        print(str(traceback.format_exc()))
    finally:

        if (
            job_hist.job_metadata.get("EXECUTION_USER_GUID")
            and job_hist.job_end_ts is None
        ):
            await Queue.enqueue_job("run_job_trigger_he_execution", job_hist)

        await update_record(
            "job_history",
            "job_end_ts",
            datetime.utcnow(),
            "job_history_pk",
            job_hist.job_history_pk,
        )


async def check_schedules(_):
    schedules_to_run = await read_schedules_to_run()
    for schedule in schedules_to_run:

        jobs = await read_jobs("job_schedule_fk", schedule.job_schedule_pk)
        job_state = await read_record(
            JobStatesEnum.QUEUED.value, "job_state", "job_state_name"
        )
        for job in jobs:
            job_hist = await create_job_history(
                JobState(**job_state).job_state_pk,
                job.job_pk,
            )
            await Queue.enqueue_job("run_job", job_hist)
        await update_schedule_next_exec(schedule)


class WorkerSettings:
    cron_jobs = [cron(check_schedules, second={0, 10, 20, 30, 40, 50})]
    functions = [
        run_job,
        run_job_post_conn,
        run_job_post_collage,
        run_job_create_he_conn,
        run_job_trigger_he_execution,
    ]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = Queue.get_settings()
    log_results = True
