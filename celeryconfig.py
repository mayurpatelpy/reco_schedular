import os
from celery.schedules import crontab

timezone = 'Asia/Kolkata'
enable_utc = False
broker_transport_options = {'region': 'ap-south-1', 'visibility_timeout': 43200, 'polling_interval': 0.3,
                            'wait_time_seconds': 20}
mongodb_backend_settings = {
    "taskmeta_collection": 'task_back_request_job'
}

broker_url = os.getenv('CELERY_BROKER_URL')
result_backend = os.getenv('CELERY_RESULT_BACKEND')
task_default_queue = "local" # No Use of this as we are using queue name while pusing
task_time_limit = 3600
task_soft_time_limit = 3600
result_backend_always_retry = True

beat_schedule = {
    'schedule-reco-scrapy-download-every-3-minute': {
        'task': 'tasks.schedule_scrapy_download_worker',
        'schedule': crontab(minute='*/3'),
    },
    'schedule-reco-scrapy-status-every-5-minute': {
        'task': 'tasks.schedule_scrapy_status_worker',
        'schedule': crontab(minute='*/5'),
    },
    'schedule-reco-scrapy-requests-every-5-minute': {
        'task': 'tasks.schedule_scrapy_requests_worker',
        'schedule': crontab(minute='*/5'),
    }
}

# this ensures that the worker acks the task after it’s completed. If the worker crashes, it will just restart.
task_acks_late = True

# this ensures that the worker process can reserve at most one un-acked task at a time. If this is used with
# ACKS_LATE=False (the default), the worker will reserve a task as soon as it starts processing the first one.
worker_prefetch_multiplier = 1

# If you have it set to True, whenever you call delay or apply_async it will just run the task synchronously instead
# of delegating it to a worker. This simplifies debugging in your local environment and facilitates automated testing.
task_always_eager = False
imports = ['tasks']
